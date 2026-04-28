from flask import Flask, render_template_string, request, jsonify, session
from flask_cors import CORS
from datetime import datetime
import sqlite3
import os
import threading
import queue
import time
import json
import secrets
import hashlib
import requests
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import logging

# ==================== APP INITIALIZATION ====================
app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
CORS(app)

# ==================== LOGGING SETUP ====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DATABASE SETUP ====================
DB_NAME = 'e2e_tool.db'

def init_db():
    """Initialize database with all tables"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Users table
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  username TEXT UNIQUE NOT NULL,
                  email TEXT UNIQUE NOT NULL,
                  password_hash TEXT NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  last_login TIMESTAMP)''')
    
    # Credentials table
    c.execute('''CREATE TABLE IF NOT EXISTS credentials
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  type TEXT,
                  value TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (user_id) REFERENCES users (id))''')
    
    # Jobs table with UID support
    c.execute('''CREATE TABLE IF NOT EXISTS jobs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  target_uid TEXT,
                  target_name TEXT,
                  delay_seconds REAL,
                  message_text TEXT,
                  status TEXT DEFAULT 'pending',
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  total_sent INTEGER DEFAULT 0,
                  total_failed INTEGER DEFAULT 0,
                  FOREIGN KEY (user_id) REFERENCES users (id))''')
    
    # Logs table
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id INTEGER,
                  log_type TEXT,
                  message TEXT,
                  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (job_id) REFERENCES jobs (id))''')
    
    # Message history table
    c.execute('''CREATE TABLE IF NOT EXISTS message_history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id INTEGER,
                  target_uid TEXT,
                  target_name TEXT,
                  message TEXT,
                  cookie_used TEXT,
                  status TEXT,
                  response TEXT,
                  sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (job_id) REFERENCES jobs (id))''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

# Initialize DB on startup
init_db()

# ==================== GLOBAL VARIABLES ====================
JOB_QUEUE = queue.Queue()
active_jobs = {}
console_logs = {}

# ==================== DECORATORS ====================
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'Please login first'}), 401
        return f(*args, **kwargs)
    return decorated_function

# ==================== FACEBOOK MESSENGER CLASS ====================
class FacebookMessenger:
    """Handle Facebook message sending via Graph API"""
    
    @staticmethod
    def get_user_info_from_uid(cookie, uid):
        """Get user info from UID using Facebook Graph API"""
        try:
            headers = {
                'Cookie': cookie,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            # Try to get user info
            url = f'https://graph.facebook.com/v18.0/{uid}'
            params = {
                'fields': 'id,name,first_name,last_name',
                'access_token': FacebookMessenger.extract_token_from_cookie(cookie)
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'uid': uid,
                    'name': data.get('name', 'Unknown'),
                    'first_name': data.get('first_name', ''),
                    'last_name': data.get('last_name', '')
                }
            else:
                return {
                    'success': False,
                    'error': f'API Error: {response.status_code}'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    @staticmethod
    def extract_token_from_cookie(cookie):
        """Extract access token from cookie string"""
        try:
            # Try to find EAA token in cookie
            if 'EAA' in cookie:
                token_start = cookie.index('EAA')
                token = cookie[token_start:].split(';')[0].split('&')[0]
                return token
            
            # Try to extract from c_user and xs
            cookies_dict = {}
            for item in cookie.split(';'):
                if '=' in item:
                    key, value = item.strip().split('=', 1)
                    cookies_dict[key.strip()] = value.strip()
            
            if 'c_user' in cookies_dict and 'xs' in cookies_dict:
                return f"{cookies_dict['c_user']}|{cookies_dict['xs']}"
            
            return None
            
        except:
            return None
    
    @staticmethod
    def send_message_to_uid(cookie, target_uid, message):
        """Send message to specific UID using Facebook Graph API"""
        try:
            headers = {
                'Cookie': cookie,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
            
            # Facebook Graph API endpoint for sending messages
            url = f'https://graph.facebook.com/v18.0/me/messages'
            
            # Prepare message data
            data = {
                'recipient': {'id': target_uid},
                'message': {'text': message},
                'messaging_type': 'RESPONSE',
                'access_token': FacebookMessenger.extract_token_from_cookie(cookie)
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=15)
            
            if response.status_code == 200:
                result = response.json()
                return {
                    'success': True,
                    'message_id': result.get('message_id', ''),
                    'recipient_id': result.get('recipient_id', target_uid)
                }
            elif response.status_code == 400:
                error_data = response.json()
                return {
                    'success': False,
                    'error': error_data.get('error', {}).get('message', 'Bad Request'),
                    'code': error_data.get('error', {}).get('code', 400)
                }
            else:
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}',
                    'response': response.text[:200]
                }
                
        except requests.exceptions.Timeout:
            return {
                'success': False,
                'error': 'Request timeout - check internet connection'
            }
        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'error': 'Connection failed - check internet'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    @staticmethod
    def validate_uid(cookie, uid):
        """Validate if UID exists and is reachable"""
        user_info = FacebookMessenger.get_user_info_from_uid(cookie, uid)
        return user_info
    
    @staticmethod
    def batch_send(cookies_list, target_uid, target_name, message, delay, job_id, log_callback):
        """Send messages using multiple cookies to single UID"""
        total = len(cookies_list)
        success = 0
        failed = 0
        
        for index, cookie in enumerate(cookies_list, 1):
            log_callback(job_id, 'info', f'📨 Using cookie {index}/{total}')
            
            # Send message
            result = FacebookMessenger.send_message_to_uid(cookie.strip(), target_uid, message)
            
            if result['success']:
                success += 1
                log_callback(job_id, 'success', 
                           f'✅ Message sent to UID {target_uid} ({target_name})')
                
                # Save to history
                conn = sqlite3.connect(DB_NAME)
                conn.execute('''INSERT INTO message_history 
                              (job_id, target_uid, target_name, message, cookie_used, status, response)
                              VALUES (?, ?, ?, ?, ?, ?, ?)''',
                           (job_id, target_uid, target_name, message, 
                            cookie[:50] + '...', 'sent', str(result.get('message_id', ''))))
                conn.commit()
                conn.close()
            else:
                failed += 1
                error_msg = result.get('error', 'Unknown error')
                log_callback(job_id, 'error', 
                           f'❌ Failed: {error_msg[:100]}')
                
                # Save failed attempt
                conn = sqlite3.connect(DB_NAME)
                conn.execute('''INSERT INTO message_history 
                              (job_id, target_uid, target_name, message, cookie_used, status, response)
                              VALUES (?, ?, ?, ?, ?, ?, ?)''',
                           (job_id, target_uid, target_name, message,
                            cookie[:50] + '...', 'failed', error_msg[:200]))
                conn.commit()
                conn.close()
            
            # Update counters
            conn = sqlite3.connect(DB_NAME)
            conn.execute('UPDATE jobs SET total_sent=?, total_failed=? WHERE id=?',
                       (success, failed, job_id))
            conn.commit()
            conn.close()
            
            # Apply delay between messages
            if index < total:
                log_callback(job_id, 'info', f'⏳ Waiting {delay}s...')
                time.sleep(delay)
        
        return success, failed

# ==================== BACKGROUND JOB PROCESSOR ====================
class JobProcessor:
    def __init__(self):
        self.running = True
        self.thread = threading.Thread(target=self.process_queue, daemon=True)
        self.thread.start()
        logger.info("Job processor started")
    
    def process_queue(self):
        while self.running:
            try:
                job_id = JOB_QUEUE.get(timeout=1)
                if job_id in active_jobs:
                    self.execute_job(job_id)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Job processing error: {e}")
                time.sleep(1)
    
    def execute_job(self, job_id):
        job = active_jobs[job_id]
        self.add_log(job_id, 'info', f'🚀 Job started for UID: {job["target_uid"]}')
        self.add_log(job_id, 'info', f'👤 Target: {job["target_name"]}')
        self.add_log(job_id, 'info', f'💬 Message: {job["message_text"][:50]}...')
        
        # Get credentials from database
        conn = sqlite3.connect(DB_NAME)
        creds = conn.execute('SELECT value FROM credentials WHERE user_id = ?', 
                            (job['user_id'],)).fetchall()
        conn.close()
        
        if not creds:
            self.add_log(job_id, 'error', '❌ No cookies/tokens found! Add credentials first.')
            self.update_job_status(job_id, 'failed')
            return
        
        credentials = [c[0] for c in creds]
        total_cookies = len(credentials)
        
        self.add_log(job_id, 'info', f'📊 Total cookies loaded: {total_cookies}')
        self.add_log(job_id, 'info', f'🎯 Target UID: {job["target_uid"]}')
        self.add_log(job_id, 'info', f'⏱️ Delay: {job["delay_seconds"]} seconds')
        
        # Infinite loop for 24/7 running
        cycle = 1
        while job_id in active_jobs and active_jobs[job_id]['status'] == 'running':
            self.add_log(job_id, 'info', f'🔄 CYCLE {cycle} STARTED')
            self.add_log(job_id, 'info', f'📤 Sending {total_cookies} messages per cycle')
            
            # Send messages using all cookies
            success, failed = FacebookMessenger.batch_send(
                credentials,
                job['target_uid'],
                job['target_name'],
                job['message_text'],
                job['delay_seconds'],
                job_id,
                self.add_log
            )
            
            self.add_log(job_id, 'success', 
                       f'✅ Cycle {cycle} Complete: {success} sent, {failed} failed')
            
            # Update overall counters
            conn = sqlite3.connect(DB_NAME)
            conn.execute('UPDATE jobs SET total_sent = total_sent + ?, total_failed = total_failed + ? WHERE id = ?',
                       (success, failed, job_id))
            conn.commit()
            conn.close()
            
            # If all failed, maybe cookies expired
            if failed == total_cookies:
                self.add_log(job_id, 'warning', 
                           '⚠️ All messages failed! Cookies might be expired.')
            
            # Wait before next cycle
            if job_id in active_jobs and active_jobs[job_id]['status'] == 'running':
                cycle += 1
                self.add_log(job_id, 'info', 
                           f'⏰ Next cycle in {job["delay_seconds"] * 2} seconds...')
                time.sleep(job['delay_seconds'] * 2)
        
        # Job finished
        final_status = 'completed' if job.get('_stopped_by_user') else 'stopped'
        self.update_job_status(job_id, final_status)
        self.add_log(job_id, 'info', f'🏁 Job {final_status}')
    
    def add_log(self, job_id, log_type, message):
        """Add log entry"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        # Save to database
        try:
            conn = sqlite3.connect(DB_NAME)
            conn.execute('INSERT INTO logs (job_id, log_type, message) VALUES (?, ?, ?)',
                        (job_id, log_type, message))
            conn.commit()
            conn.close()
        except:
            pass
        
        # Save to memory for real-time console
        if job_id not in console_logs:
            console_logs[job_id] = []
        console_logs[job_id].append({
            'timestamp': timestamp,
            'type': log_type,
            'message': message
        })
        
        # Keep only last 500 logs in memory
        if len(console_logs[job_id]) > 500:
            console_logs[job_id] = console_logs[job_id][-500:]
        
        logger.info(f"[Job {job_id}] {message}")
    
    def update_job_status(self, job_id, status):
        """Update job status in database"""
        try:
            conn = sqlite3.connect(DB_NAME)
            conn.execute('UPDATE jobs SET status = ? WHERE id = ?', (status, job_id))
            conn.commit()
            conn.close()
        except:
            pass

# Start background processor
job_processor = JobProcessor()

# ==================== HTML TEMPLATE ====================
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>End to End Offline Tool by Virat Rajput</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        :root {
            --primary: #1565C0;
            --light: #42A5F5;
            --neon: #00BCD4;
            --bg: linear-gradient(135deg, #ffffff 0%, #E3F2FD 50%, #BBDEFB 100%);
            --card-shadow: 0 8px 32px rgba(21,101,192,0.1);
            --neon-glow: 0 0 20px rgba(0,188,212,0.3);
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', sans-serif;
            background: var(--bg);
            min-height: 100vh;
        }
        
        .header {
            background: linear-gradient(135deg, #0D47A1, #1565C0, #1976D2);
            color: white;
            padding: 25px;
            text-align: center;
            box-shadow: 0 4px 20px rgba(0,0,0,0.2);
            position: relative;
            overflow: hidden;
        }
        
        .header::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: linear-gradient(45deg, transparent, rgba(255,255,255,0.1), transparent);
            animation: shine 3s infinite;
        }
        
        @keyframes shine {
            0% { transform: translateX(-100%) rotate(45deg); }
            100% { transform: translateX(100%) rotate(45deg); }
        }
        
        .header h1 {
            font-size: 2.2em;
            font-weight: 800;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            position: relative;
            z-index: 1;
        }
        
        .header h3 {
            color: var(--neon);
            text-shadow: var(--neon-glow);
            position: relative;
            z-index: 1;
        }
        
        .container {
            max-width: 1400px;
            margin: 20px auto;
            padding: 20px;
        }
        
        .card {
            background: rgba(255,255,255,0.95);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            box-shadow: var(--card-shadow);
            border: 1px solid rgba(33,150,243,0.1);
            margin-bottom: 25px;
            transition: all 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-3px);
            box-shadow: 0 12px 40px rgba(21,101,192,0.15);
        }
        
        .card-header {
            background: linear-gradient(135deg, #1565C0, #1976D2);
            color: white;
            border-radius: 20px 20px 0 0;
            padding: 15px 25px;
            font-weight: 700;
            font-size: 1.1em;
        }
        
        .card-body { padding: 25px; }
        
        .form-label {
            font-weight: 600;
            color: #1565C0;
            margin-bottom: 8px;
        }
        
        .form-control {
            border: 2px solid #E3F2FD;
            border-radius: 12px;
            padding: 12px;
            transition: all 0.3s ease;
        }
        
        .form-control:focus {
            border-color: var(--light);
            box-shadow: 0 0 0 0.2rem rgba(66,165,245,0.15);
        }
        
        textarea.form-control { min-height: 100px; resize: vertical; }
        
        .btn {
            padding: 12px 30px;
            font-weight: 600;
            border-radius: 12px;
            transition: all 0.3s ease;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, #1565C0, #1976D2);
            border: none;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(21,101,192,0.4);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #D32F2F, #F44336);
            border: none;
            animation: glow-red 2s infinite;
        }
        
        @keyframes glow-red {
            0%, 100% { box-shadow: 0 0 20px rgba(244,67,54,0.4); }
            50% { box-shadow: 0 0 40px rgba(244,67,54,0.8); }
        }
        
        .btn-danger:hover { transform: scale(1.05); }
        
        .btn-warning {
            background: linear-gradient(135deg, #F57C00, #FF9800);
            border: none;
            animation: glow-orange 2s infinite;
        }
        
        @keyframes glow-orange {
            0%, 100% { box-shadow: 0 0 20px rgba(255,152,0,0.4); }
            50% { box-shadow: 0 0 40px rgba(255,152,0,0.8); }
        }
        
        .btn-warning:hover { transform: scale(1.05); }
        
        .btn-success {
            background: linear-gradient(135deg, #388E3C, #4CAF50);
            border: none;
        }
        
        .status-badge {
            display: inline-block;
            padding: 6px 15px;
            border-radius: 25px;
            font-weight: 700;
            font-size: 0.9em;
            text-transform: uppercase;
        }
        
        .status-running {
            background: linear-gradient(135deg, #4CAF50, #66BB6A);
            color: white;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.8; }
        }
        
        .status-stopped {
            background: linear-gradient(135deg, #9E9E9E, #BDBDBD);
            color: white;
        }
        
        .console {
            background: #0A1929;
            color: #00FF41;
            font-family: 'Courier New', monospace;
            padding: 20px;
            border-radius: 15px;
            height: 400px;
            overflow-y: auto;
            border: 2px solid var(--neon);
            box-shadow: inset 0 0 30px rgba(0,188,212,0.1);
        }
        
        .console .log-entry {
            padding: 5px 0;
            border-bottom: 1px solid rgba(0,255,65,0.05);
            animation: fadeIn 0.3s ease;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateX(-10px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .console .timestamp { color: #FFD700; font-weight: bold; }
        .console .success { color: #4CAF50; }
        .console .error { color: #F44336; }
        .console .warning { color: #FF9800; }
        .console .info { color: #2196F3; }
        
        .e2e-info {
            background: linear-gradient(135deg, #E8EAF6, #C5CAE9);
            border-radius: 15px;
            padding: 25px;
            margin: 25px 0;
            border-left: 5px solid var(--primary);
        }
        
        .upload-zone {
            border: 3px dashed #90CAF9;
            border-radius: 15px;
            padding: 30px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s ease;
            background: rgba(227,242,253,0.5);
        }
        
        .upload-zone:hover {
            border-color: var(--primary);
            background: rgba(21,101,192,0.05);
            transform: scale(1.02);
        }
        
        .upload-zone i { font-size: 3em; color: var(--primary); }
        
        .uid-badge {
            background: linear-gradient(135deg, #FF6F00, #FF8F00);
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 600;
        }
        
        .footer {
            background: linear-gradient(135deg, #0D47A1, #1565C0);
            color: white;
            text-align: center;
            padding: 30px;
            margin-top: 50px;
        }
        
        .footer p { margin: 5px 0; }
        
        .neon-text {
            color: var(--neon);
            text-shadow: var(--neon-glow);
        }
        
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #0A1929; border-radius: 10px; }
        ::-webkit-scrollbar-thumb { background: var(--neon); border-radius: 10px; }
        
        @media (max-width: 768px) {
            .header h1 { font-size: 1.5em; }
            .btn { width: 100%; margin-bottom: 10px; }
            .console { height: 300px; }
        }
    </style>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <h1>🔐 End to End Offline Tool by Virat Rajput</h1>
        <h3>🚀 Offline Tool Non-Stop E2E UID Messenger</h3>
    </div>
    
    <div class="container">
        <!-- Auth Section -->
        <div id="auth-section">
            <div class="card">
                <div class="card-header">
                    <span><i class="fas fa-lock"></i> Account Access</span>
                    <div>
                        <button class="btn btn-sm btn-light" onclick="showRegister()" id="register-btn">Create Account</button>
                        <button class="btn btn-sm btn-light" onclick="showLogin()" id="login-btn" style="display:none;">Login</button>
                    </div>
                </div>
                <div class="card-body">
                    <!-- Login Form -->
                    <div id="login-form">
                        <h4 class="text-center mb-4" style="color: var(--primary);">Welcome Back</h4>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-user"></i> Username</label>
                            <input type="text" class="form-control" id="login-username" placeholder="Enter username">
                        </div>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-key"></i> Password</label>
                            <input type="password" class="form-control" id="login-password" placeholder="Min 8 characters">
                        </div>
                        <button class="btn btn-primary w-100" onclick="login()">
                            <i class="fas fa-sign-in-alt"></i> Login
                        </button>
                    </div>
                    
                    <!-- Register Form -->
                    <div id="register-form" style="display:none;">
                        <h4 class="text-center mb-4" style="color: var(--primary);">Create Account</h4>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-envelope"></i> Email ID</label>
                            <input type="email" class="form-control" id="reg-email" placeholder="Enter email">
                        </div>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-user"></i> Username</label>
                            <input type="text" class="form-control" id="reg-username" placeholder="Choose username">
                        </div>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-key"></i> Password (Min 8 chars)</label>
                            <input type="password" class="form-control" id="reg-password" placeholder="Min 8 characters">
                        </div>
                        <div class="mb-3">
                            <label class="form-label"><i class="fas fa-key"></i> Confirm Password</label>
                            <input type="password" class="form-control" id="reg-confirm" placeholder="Re-enter password">
                        </div>
                        <button class="btn btn-primary w-100" onclick="register()">
                            <i class="fas fa-user-plus"></i> Create Account
                        </button>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Dashboard Section -->
        <div id="dashboard-section" style="display:none;">
            <!-- User Info -->
            <div class="card">
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-center">
                        <h5 style="color: var(--primary);">
                            <i class="fas fa-user-circle"></i> Welcome, <span id="username-display"></span>
                        </h5>
                        <div>
                            <span class="status-badge status-stopped" id="job-status">
                                <i class="fas fa-circle"></i> No Active Job
                            </span>
                            <button class="btn btn-sm btn-danger ms-3" onclick="logout()">
                                <i class="fas fa-sign-out-alt"></i> Logout
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- E2E Info -->
            <div class="e2e-info">
                <h4><i class="fas fa-shield-alt"></i> End-to-End Encryption (E2EE) + UID Messaging</h4>
                <p style="line-height: 1.8; margin-top: 15px;">
                    <strong>E2EE</strong> ensures only sender and receiver can read messages. With <strong>UID-based messaging</strong>, you can target specific Facebook users using their unique User ID. Messages are encrypted end-to-end and sent directly to the recipient's inbox.
                </p>
                <p style="line-height: 1.8;">
                    <strong>UID Messaging Features:</strong>
                    <br>• Send messages to specific Facebook User ID
                    <br>• Multiple cookies support for continuous sending
                    <br>• 24/7 non-stop operation
                    <br>• Automatic retry on failure
                    <br>• Real-time delivery tracking
                </p>
            </div>
            
            <!-- UID Lookup -->
            <div class="card">
                <div class="card-header">
                    <i class="fas fa-search"></i> UID Lookup & Validation
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6 mb-3">
                            <label class="form-label"><i class="fas fa-id-card"></i> Facebook UID</label>
                            <input type="text" class="form-control" id="lookup-uid" placeholder="Enter Facebook User ID (e.g., 1000xxxxxxxxx)">
                        </div>
                        <div class="col-md-6 mb-3">
                            <label class="form-label"><i class="fas fa-cookie"></i> Cookie for Validation</label>
                            <input type="text" class="form-control" id="lookup-cookie" placeholder="Paste a valid cookie">
                        </div>
                    </div>
                    <button class="btn btn-success" onclick="validateUID()">
                        <i class="fas fa-check-circle"></i> Validate UID
                    </button>
                    <div id="uid-result" class="mt-3" style="display:none;"></div>
                </div>
            </div>
            
            <!-- Credentials & Upload -->
            <div class="row">
                <div class="col-md-6 mb-4">
                    <div class="card h-100">
                        <div class="card-header"><i class="fas fa-cookie-bite"></i> Cookies & Tokens</div>
                        <div class="card-body">
                            <div class="mb-3">
                                <label class="form-label">Cookies (One per line)</label>
                                <textarea class="form-control" id="cookies-input" rows="3" placeholder="cookie1=value1; c_user=...; xs=..."></textarea>
                            </div>
                            <div class="mb-3">
                                <label class="form-label">Tokens (One per line)</label>
                                <textarea class="form-control" id="tokens-input" rows="3" placeholder="EAAxxxxx..."></textarea>
                            </div>
                            <button class="btn btn-primary" onclick="addCredentials()">
                                <i class="fas fa-plus"></i> Add Credentials
                            </button>
                        </div>
                    </div>
                </div>
                
                <div class="col-md-6 mb-4">
                    <div class="card h-100">
                        <div class="card-header"><i class="fas fa-file-upload"></i> Upload Files</div>
                        <div class="card-body">
                            <div class="upload-zone mb-3" onclick="document.getElementById('cred-file').click()">
                                <i class="fas fa-cloud-upload-alt"></i>
                                <p>Upload Cookies File (.txt)</p>
                                <input type="file" id="cred-file" accept=".txt" style="display:none;" onchange="uploadCredFile(this)">
                            </div>
                            <div class="upload-zone" onclick="document.getElementById('msg-file').click()">
                                <i class="fas fa-file-alt"></i>
                                <p>Upload Message File (.txt)</p>
                                <input type="file" id="msg-file" accept=".txt" style="display:none;" onchange="uploadMsgFile(this)">
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Job Config with UID -->
            <div class="card">
                <div class="card-header">
                    <i class="fas fa-cogs"></i> UID Message Configuration
                    <span id="job-id-display" class="uid-badge" style="display:none;">Job: -</span>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-4 mb-3">
                            <label class="form-label"><i class="fas fa-fingerprint"></i> Target UID (Required)</label>
                            <input type="text" class="form-control" id="target-uid" placeholder="Facebook User ID">
                            <small class="text-muted">Example: 1000123456789</small>
                        </div>
                        <div class="col-md-4 mb-3">
                            <label class="form-label"><i class="fas fa-user-slash"></i> Hater's Name</label>
                            <input type="text" class="form-control" id="target-name" placeholder="Name for reference">
                        </div>
                        <div class="col-md-4 mb-3">
                            <label class="form-label"><i class="fas fa-clock"></i> Delay (Seconds)</label>
                            <input type="number" class="form-control" id="delay" value="5" min="1">
                            <small class="text-muted">Time between messages</small>
                        </div>
                    </div>
                    <div class="mb-4">
                        <label class="form-label"><i class="fas fa-envelope"></i> Message</label>
                        <textarea class="form-control" id="message-text" rows="3" placeholder="Type your message to send..."></textarea>
                    </div>
                    <div class="d-flex gap-3">
                        <button class="btn btn-danger" onclick="startJob()">
                            <i class="fas fa-play"></i> START 24/7 SENDING
                        </button>
                        <button class="btn btn-warning" onclick="stopJob()">
                            <i class="fas fa-stop"></i> STOP
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- Console -->
            <div class="card">
                <div class="card-header">
                    <i class="fas fa-terminal"></i> Live Console (24/7 Monitoring)
                    <div>
                        <button class="btn btn-sm btn-light" onclick="clearConsole()">
                            <i class="fas fa-eraser"></i> Clear
                        </button>
                        <button class="btn btn-sm btn-success" onclick="exportLogs()">
                            <i class="fas fa-download"></i> Export
                        </button>
                    </div>
                </div>
                <div class="card-body">
                    <div class="console" id="console">
                        <div class="log-entry">
                            <span class="timestamp">[System]</span>
                            <span class="info">🔧 UID Messenger Ready. Configure target UID and start!</span>
                        </div>
                        <div class="log-entry">
                            <span class="timestamp">[System]</span>
                            <span class="info">📡 Server running 24/7 - Auto-recovery enabled</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Message History -->
            <div class="card">
                <div class="card-header">
                    <i class="fas fa-history"></i> Recent Message History
                </div>
                <div class="card-body">
                    <div class="table-responsive">
                        <table class="table table-hover" id="history-table">
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>UID</th>
                                    <th>Name</th>
                                    <th>Message</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody id="history-body">
                                <tr><td colspan="5" class="text-center text-muted">No messages sent yet</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Footer -->
    <div class="footer">
        <p><strong>Made by Virat Rajput (Software Developer)</strong></p>
        <p>End to End Offline Server | UID Messenger System</p>
        <p>All rights reserved 2026</p>
        <p class="neon-text">
            <i class="fas fa-circle" style="color: #4CAF50;"></i> Running 24/7 |
            <i class="fas fa-shield-alt"></i> Secure E2E |
            <i class="fas fa-bolt"></i> High Performance |
            <i class="fas fa-fingerprint"></i> UID Enabled
        </p>
    </div>
    
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script>
        let currentJobId = null;
        let consoleInterval = null;
        
        function showRegister() {
            $('#login-form').hide();
            $('#register-form').show();
            $('#register-btn').hide();
            $('#login-btn').show();
        }
        
        function showLogin() {
            $('#register-form').hide();
            $('#login-form').show();
            $('#login-btn').hide();
            $('#register-btn').show();
        }
        
        async function register() {
            const email = $('#reg-email').val().trim();
            const username = $('#reg-username').val().trim();
            const password = $('#reg-password').val();
            const confirm = $('#reg-confirm').val();
            
            if (!email || !username || !password || !confirm) {
                return alert('Please fill all fields');
            }
            if (password.length < 8) {
                return alert('Password must be at least 8 characters');
            }
            if (password !== confirm) {
                return alert('Passwords do not match');
            }
            
            try {
                const res = await fetch('/api/register', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({email, username, password, confirm_password: confirm})
                });
                const data = await res.json();
                alert(data.message || data.error);
                if (data.success) showLogin();
            } catch(e) {
                alert('Registration failed');
            }
        }
        
        async function login() {
            const username = $('#login-username').val().trim();
            const password = $('#login-password').val();
            
            if (!username || !password) {
                return alert('Please enter username and password');
            }
            
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({username, password})
                });
                const data = await res.json();
                if (data.success) {
                    $('#auth-section').hide();
                    $('#dashboard-section').show();
                    $('#username-display').text(data.user.username);
                    loadJobs();
                } else {
                    alert(data.error);
                }
            } catch(e) {
                alert('Login failed');
            }
        }
        
        async function logout() {
            await fetch('/api/logout', {method: 'POST'});
            location.reload();
        }
        
        async function validateUID() {
            const uid = $('#lookup-uid').val().trim();
            const cookie = $('#lookup-cookie').val().trim();
            
            if (!uid || !cookie) {
                return alert('Enter UID and a cookie for validation');
            }
            
            try {
                const res = await fetch('/api/uid/validate', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({uid, cookie})
                });
                const data = await res.json();
                
                const resultDiv = $('#uid-result');
                resultDiv.show();
                
                if (data.success) {
                    resultDiv.html(`
                        <div class="alert alert-success">
                            <strong>✅ Valid UID!</strong><br>
                            Name: ${data.user_info.name}<br>
                            UID: ${data.user_info.uid}<br>
                            <small>You can now send messages to this UID</small>
                        </div>
                    `);
                    // Auto-fill target UID
                    $('#target-uid').val(uid);
                    if (data.user_info.name !== 'Unknown') {
                        $('#target-name').val(data.user_info.name);
                    }
                } else {
                    resultDiv.html(`
                        <div class="alert alert-danger">
                            <strong>❌ Invalid:</strong> ${data.error}
                        </div>
                    `);
                }
            } catch(e) {
                alert('UID validation failed');
            }
        }
        
        async function addCredentials() {
            const cookies = $('#cookies-input').val();
            const tokens = $('#tokens-input').val();
            
            if (!cookies && !tokens) return alert('Enter cookies or tokens');
            
            try {
                const res = await fetch('/api/credentials/add', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({cookies, tokens})
                });
                const data = await res.json();
                alert(data.message);
                if (data.success) {
                    $('#cookies-input, #tokens-input').val('');
                }
            } catch(e) {
                alert('Failed to add credentials');
            }
        }
        
        function uploadCredFile(input) {
            const file = input.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = function(e) {
                $('#cookies-input').val(e.target.result);
            };
            reader.readAsText(file);
        }
        
        function uploadMsgFile(input) {
            const file = input.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = function(e) {
                $('#message-text').val(e.target.result);
            };
            reader.readAsText(file);
        }
        
        async function startJob() {
            const targetUid = $('#target-uid').val().trim();
            const targetName = $('#target-name').val().trim();
            const delay = $('#delay').val();
            const message = $('#message-text').val().trim();
            
            if (!targetUid) return alert('Target UID is required!');
            if (!message) return alert('Enter message to send');
            if (!targetName) return alert('Enter target name for reference');
            
            try {
                const res = await fetch('/api/jobs/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        target_uid: targetUid,
                        target_name: targetName,
                        delay: parseFloat(delay),
                        message: message
                    })
                });
                const data = await res.json();
                if (data.success) {
                    currentJobId = data.job_id;
                    $('#job-status')
                        .removeClass('status-stopped')
                        .addClass('status-running')
                        .html('<i class="fas fa-spinner fa-spin"></i> 24/7 Running');
                    $('#job-id-display').show().text('Job: ' + currentJobId);
                    startConsole();
                    alert('✅ 24/7 messaging started for UID: ' + targetUid);
                } else {
                    alert(data.error);
                }
            } catch(e) {
                alert('Failed to start job');
            }
        }
        
        async function stopJob() {
            if (!currentJobId) return alert('No active job');
            
            try {
                const res = await fetch('/api/jobs/stop', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({job_id: currentJobId})
                });
                const data = await res.json();
                if (data.success) {
                    $('#job-status')
                        .removeClass('status-running')
                        .addClass('status-stopped')
                        .html('<i class="fas fa-circle"></i> Stopped');
                    $('#job-id-display').hide();
                    stopConsole();
                    currentJobId = null;
                    alert('Job stopped');
                }
            } catch(e) {
                alert('Failed to stop job');
            }
        }
        
        function startConsole() {
            if (consoleInterval) return;
            consoleInterval = setInterval(updateConsole, 1000);
        }
        
        function stopConsole() {
            if (consoleInterval) {
                clearInterval(consoleInterval);
                consoleInterval = null;
            }
        }
        
        async function updateConsole() {
            if (!currentJobId) return;
            
            try {
                const res = await fetch('/api/jobs/logs/' + currentJobId);
                const data = await res.json();
                if (data.success && data.logs) {
                    const consoleDiv = $('#console');
                    consoleDiv.empty();
                    data.logs.forEach(log => {
                        consoleDiv.append(`
                            <div class="log-entry">
                                <span class="timestamp">[${log.timestamp}]</span>
                                <span class="${log.type}">${log.message}</span>
                            </div>
                        `);
                    });
                    consoleDiv.scrollTop(consoleDiv[0].scrollHeight);
                }
                
                // Update history table
                const histRes = await fetch('/api/messages/history/' + currentJobId);
                const histData = await histRes.json();
                if (histData.success && histData.messages) {
                    const tbody = $('#history-body');
                    tbody.empty();
                    histData.messages.slice(0, 10).forEach(msg => {
                        const statusClass = msg.status === 'sent' ? 'success' : 'danger';
                        tbody.append(`
                            <tr>
                                <td><small>${msg.sent_at}</small></td>
                                <td><code>${msg.target_uid}</code></td>
                                <td>${msg.target_name}</td>
                                <td>${msg.message.substring(0, 30)}...</td>
                                <td><span class="badge bg-${statusClass}">${msg.status}</span></td>
                            </tr>
                        `);
                    });
                }
            } catch(e) {
                console.error('Console error:', e);
            }
        }
        
        function clearConsole() {
            $('#console').html(`
                <div class="log-entry">
                    <span class="timestamp">[System]</span>
                    <span class="info">Console cleared</span>
                </div>
            `);
        }
        
        function exportLogs() {
            if (!currentJobId) return alert('No active job');
            window.open('/api/jobs/logs/' + currentJobId + '?format=text', '_blank');
        }
        
        async function loadJobs() {
            try {
                const res = await fetch('/api/jobs/status');
                const data = await res.json();
                if (data.success && data.jobs) {
                    const running = data.jobs.find(j => j.status === 'running');
                    if (running) {
                        currentJobId = running.id;
                        $('#job-status')
                            .removeClass('status-stopped')
                            .addClass('status-running')
                            .html('<i class="fas fa-spinner fa-spin"></i> 24/7 Running');
                        $('#job-id-display').show().text('Job: ' + currentJobId);
                        startConsole();
                    }
                }
            } catch(e) {
                console.error('Load jobs error:', e);
            }
        }
        
        // Health check
        setInterval(async () => {
            try {
                await fetch('/api/health');
            } catch(e) {}
        }, 30000);
    </script>
</body>
</html>
'''

# ==================== API ROUTES ====================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        email = data.get('email', '').strip()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        confirm = data.get('confirm_password', '')
        
        if not all([email, username, password, confirm]):
            return jsonify({'error': 'All fields required'}), 400
        if len(password) < 8:
            return jsonify({'error': 'Password min 8 characters'}), 400
        if password != confirm:
            return jsonify({'error': 'Passwords do not match'}), 400
        
        conn = sqlite3.connect(DB_NAME)
        existing = conn.execute('SELECT id FROM users WHERE username=? OR email=?',
                               (username, email)).fetchone()
        if existing:
            conn.close()
            return jsonify({'error': 'Username/Email already exists'}), 400
        
        hash_pw = generate_password_hash(password)
        conn.execute('INSERT INTO users (username, email, password_hash) VALUES (?,?,?)',
                    (username, email, hash_pw))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Account created! Please login.'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        
        conn = sqlite3.connect(DB_NAME)
        user = conn.execute('SELECT * FROM users WHERE username=?', (username,)).fetchone()
        conn.close()
        
        if user and check_password_hash(user[3], password):
            session['user_id'] = user[0]
            session['username'] = user[1]
            return jsonify({
                'success': True,
                'user': {'id': user[0], 'username': user[1], 'email': user[2]}
            })
        return jsonify({'error': 'Invalid credentials'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True})

@app.route('/api/uid/validate', methods=['POST'])
@login_required
def validate_uid():
    try:
        data = request.get_json()
        uid = data.get('uid', '').strip()
        cookie = data.get('cookie', '').strip()
        
        if not uid or not cookie:
            return jsonify({'error': 'UID and cookie required'}), 400
        
        # Validate UID format
        if not uid.isdigit() or len(uid) < 5:
            return jsonify({'error': 'Invalid UID format'}), 400
        
        # Check UID
        result = FacebookMessenger.validate_uid(cookie, uid)
        
        if result['success']:
            return jsonify({
                'success': True,
                'user_info': result
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Validation failed')
            })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/credentials/add', methods=['POST'])
@login_required
def add_credentials():
    try:
        data = request.get_json()
        cookies = data.get('cookies', '')
        tokens = data.get('tokens', '')
        
        conn = sqlite3.connect(DB_NAME)
        count = 0
        
        for line in cookies.split('\n'):
            line = line.strip()
            if line:
                conn.execute('INSERT INTO credentials (user_id, type, value) VALUES (?,?,?)',
                           (session['user_id'], 'cookie', line))
                count += 1
        
        for line in tokens.split('\n'):
            line = line.strip()
            if line:
                conn.execute('INSERT INTO credentials (user_id, type, value) VALUES (?,?,?)',
                           (session['user_id'], 'token', line))
                count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Added {count} credentials'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs/start', methods=['POST'])
@login_required
def start_job():
    try:
        data = request.get_json()
        target_uid = data.get('target_uid', '').strip()
        target_name = data.get('target_name', '').strip()
        delay = float(data.get('delay', 5))
        message = data.get('message', '').strip()
        
        if not target_uid:
            return jsonify({'error': 'Target UID is required'}), 400
        if not target_name:
            return jsonify({'error': 'Target name is required'}), 400
        if not message:
            return jsonify({'error': 'Message is required'}), 400
        
        # Validate UID format
        if not target_uid.isdigit():
            return jsonify({'error': 'Invalid UID format - must be numbers only'}), 400
        
        conn = sqlite3.connect(DB_NAME)
        
        # Check running jobs
        running = conn.execute(
            'SELECT id FROM jobs WHERE user_id=? AND status="running"',
            (session['user_id'],)).fetchone()
        if running:
            conn.close()
            return jsonify({'error': 'Stop current job first'}), 400
        
        # Check credentials exist
        cred_count = conn.execute(
            'SELECT COUNT(*) FROM credentials WHERE user_id=?',
            (session['user_id'],)).fetchone()[0]
        
        if cred_count == 0:
            conn.close()
            return jsonify({'error': 'No cookies/tokens added! Please add credentials first.'}), 400
        
        # Create job with UID
        conn.execute('''INSERT INTO jobs 
                       (user_id, target_uid, target_name, delay_seconds, message_text, status)
                       VALUES (?, ?, ?, ?, ?, ?)''',
                    (session['user_id'], target_uid, target_name, delay, message, 'pending'))
        job_id = conn.lastrowid
        conn.commit()
        conn.close()
        
        # Store in memory
        active_jobs[job_id] = {
            'user_id': session['user_id'],
            'target_uid': target_uid,
            'target_name': target_name,
            'delay_seconds': delay,
            'message_text': message,
            'status': 'pending'
        }
        
        # Initialize logs
        console_logs[job_id] = [{
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'type': 'info',
            'message': f'🎯 Job created for UID: {target_uid} ({target_name})'
        }]
        
        # Add to queue
        JOB_QUEUE.put(job_id)
        
        # Update status
        conn = sqlite3.connect(DB_NAME)
        conn.execute('UPDATE jobs SET status="running" WHERE id=?', (job_id,))
        conn.commit()
        conn.close()
        
        if job_id in active_jobs:
            active_jobs[job_id]['status'] = 'running'
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'24/7 messaging started for UID: {target_uid}'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs/stop', methods=['POST'])
@login_required
def stop_job():
    try:
        data = request.get_json()
        job_id = data.get('job_id')
        
        if job_id and job_id in active_jobs:
            active_jobs[job_id]['status'] = 'stopped'
            active_jobs[job_id]['_stopped_by_user'] = True
            conn = sqlite3.connect(DB_NAME)
            conn.execute('UPDATE jobs SET status="stopped" WHERE id=?', (job_id,))
            conn.commit()
            conn.close()
            del active_jobs[job_id]
        else:
            conn = sqlite3.connect(DB_NAME)
            conn.execute('UPDATE jobs SET status="stopped" WHERE user_id=? AND status="running"',
                        (session['user_id'],))
            conn.commit()
            conn.close()
            for jid in list(active_jobs.keys()):
                if active_jobs[jid]['user_id'] == session['user_id']:
                    active_jobs[jid]['status'] = 'stopped'
                    active_jobs[jid]['_stopped_by_user'] = True
                    del active_jobs[jid]
        
        return jsonify({'success': True, 'message': 'Job stopped successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs/logs/<int:job_id>')
@login_required
def get_logs(job_id):
    logs = console_logs.get(job_id, [])
    if not logs:
        conn = sqlite3.connect(DB_NAME)
        db_logs = conn.execute(
            'SELECT * FROM logs WHERE job_id=? ORDER BY timestamp DESC LIMIT 50',
            (job_id,)).fetchall()
        conn.close()
        logs = [{
            'timestamp': l[4][:19].split(' ')[-1] if len(l) > 4 else '',
            'type': l[2] if len(l) > 2 else 'info',
            'message': l[3] if len(l) > 3 else ''
        } for l in db_logs]
    
    return jsonify({'success': True, 'logs': logs[-100:]})

@app.route('/api/jobs/status')
@login_required
def job_status():
    conn = sqlite3.connect(DB_NAME)
    jobs = conn.execute(
        'SELECT * FROM jobs WHERE user_id=? ORDER BY created_at DESC LIMIT 10',
        (session['user_id'],)).fetchall()
    conn.close()
    
    return jsonify({
        'success': True,
        'jobs': [{
            'id': j[0],
            'target_uid': j[2],
            'target_name': j[3],
            'status': j[6],
            'total_sent': j[7],
            'total_failed': j[8]
        } for j in jobs]
    })

@app.route('/api/messages/history/<int:job_id>')
@login_required
def get_message_history(job_id):
    conn = sqlite3.connect(DB_NAME)
    messages = conn.execute(
        'SELECT * FROM message_history WHERE job_id=? ORDER BY sent_at DESC LIMIT 20',
        (job_id,)).fetchall()
    conn.close()
    
    return jsonify({
        'success': True,
        'messages': [{
            'id': m[0],
            'target_uid': m[2],
            'target_name': m[3],
            'message': m[4],
            'status': m[6],
            'sent_at': m[8]
        } for m in messages]
    })

@app.route('/api/health')
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_jobs': len(active_jobs),
        'mode': 'UID Messenger 24/7'
    })

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"UID Messenger Server starting on port {port}")
    logger.info("Features: UID-based messaging | 24/7 operation | Multiple cookies")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
