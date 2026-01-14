"""Flask Web Application for ICANN Downloader."""
import logging
from datetime import datetime
from typing import Optional
from functools import wraps

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

from src.config import Config
from src.services.download_service import DownloadService
from src.services.scheduler_service import SchedulerService
from src.services.logger_service import LoggerService
from src.services.db_repository import ClickHouseRepository


logger = logging.getLogger(__name__)


def create_app(
    config: Optional[Config] = None,
    download_service: Optional[DownloadService] = None,
    scheduler_service: Optional[SchedulerService] = None,
    logger_service: Optional[LoggerService] = None,
    repository: Optional[ClickHouseRepository] = None,
) -> tuple:
    """Create Flask application with dependencies.
    
    Args:
        config: Application configuration
        download_service: Download service instance
        scheduler_service: Scheduler service instance
        logger_service: Logger service instance
        repository: ClickHouse repository instance
        
    Returns:
        Tuple of (Flask app, SocketIO instance)
    """
    app = Flask(__name__, template_folder='../../templates')
    app.config['SECRET_KEY'] = 'icann-downloader-secret'
    
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
    
    # Store services in app context
    app.download_service = download_service
    app.scheduler_service = scheduler_service
    app.logger_service = logger_service
    app.repository = repository
    app.config_obj = config
    
    # Update logger service with socketio
    if logger_service:
        logger_service.socketio = socketio
    
    def require_services(*services):
        """Decorator to check if required services are available."""
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                for service_name in services:
                    if getattr(app, service_name, None) is None:
                        return jsonify({
                            "error": f"Service {service_name} not available"
                        }), 503
                return f(*args, **kwargs)
            return decorated_function
        return decorator
    
    # Routes
    @app.route('/')
    def index():
        """Render landing page."""
        return render_template('index.html')
    
    @app.route('/dashboard')
    def dashboard():
        """Render dashboard page with current status."""
        return render_template('dashboard.html')
    
    @app.route('/api/status')
    def get_status():
        """Get current system status."""
        status = {
            "timestamp": datetime.now().isoformat(),
            "job": None,
            "scheduler": None,
            "last_download": None,
            "total_domains_processed": 0,
        }
        
        # Get job status
        if app.download_service:
            job_status = app.download_service.get_current_status()
            status["job"] = {
                "state": job_status.state,
                "current_tld": job_status.current_tld,
                "progress_percent": job_status.progress_percent,
                "total_tlds": job_status.total_tlds,
                "completed_tlds": job_status.completed_tlds,
                "started_at": job_status.started_at.isoformat() if job_status.started_at else None,
            }
            status["active_jobs"] = 1 if job_status.is_running else 0
        
        # Get scheduler status
        if app.scheduler_service:
            status["scheduler"] = app.scheduler_service.get_status()
        
        # Get last download info from database
        if app.repository:
            try:
                logs = app.repository.get_recent_logs(limit=1)
                if logs:
                    last_log = logs[0]
                    status["last_download"] = {
                        "tld": last_log.tld,
                        "status": last_log.status,
                        "records_count": last_log.records_count,
                        "completed_at": last_log.completed_at.isoformat() if last_log.completed_at else None,
                    }
                    status["last_download_time"] = last_log.completed_at.isoformat() if last_log.completed_at else None
                
                # Get total domains processed (approximate)
                total_logs = app.repository.get_recent_logs(limit=1000)
                status["total_domains_processed"] = sum(
                    log.records_count for log in total_logs if log.status == "success"
                )
            except Exception as e:
                logger.warning(f"Failed to get download stats: {e}")
        
        return jsonify(status)
    
    @app.route('/api/download', methods=['POST'])
    @require_services('download_service')
    def trigger_download():
        """Trigger manual download."""
        if app.download_service.is_running():
            return jsonify({
                "error": "Download already in progress",
                "status": "rejected"
            }), 409
        
        # Start download in background thread
        import threading
        
        def run_download():
            try:
                app.download_service.run_full_download()
            except Exception as e:
                logger.error(f"Download failed: {e}")
        
        thread = threading.Thread(target=run_download, daemon=True)
        thread.start()
        
        return jsonify({
            "status": "started",
            "message": "Download started in background"
        })
    
    @app.route('/api/auto-download', methods=['POST'])
    @require_services('scheduler_service')
    def toggle_auto_download():
        """Enable/disable automatic downloads."""
        data = request.get_json() or {}
        enabled = data.get('enabled')
        
        if enabled is None:
            # Toggle current state
            enabled = not app.scheduler_service.is_enabled()
        
        if enabled:
            app.scheduler_service.enable_auto_download()
        else:
            app.scheduler_service.disable_auto_download()
        
        return jsonify({
            "enabled": app.scheduler_service.is_enabled(),
            "next_run_time": (
                app.scheduler_service.get_next_run_time().isoformat()
                if app.scheduler_service.get_next_run_time()
                else None
            ),
        })
    
    @app.route('/api/logs')
    def get_logs():
        """Get recent log entries."""
        limit = request.args.get('limit', 100, type=int)
        limit = min(limit, 500)  # Cap at 500
        
        logs = []
        
        # Get in-memory logs from logger service
        if app.logger_service:
            logs = app.logger_service.get_logs_as_dicts(limit)
        
        return jsonify({
            "logs": logs,
            "count": len(logs),
        })
    
    @app.route('/api/download-logs')
    def get_download_logs():
        """Get download logs from database."""
        limit = request.args.get('limit', 100, type=int)
        limit = min(limit, 500)
        
        logs = []
        
        if app.repository:
            try:
                db_logs = app.repository.get_recent_logs(limit)
                logs = [
                    {
                        "id": log.id,
                        "tld": log.tld,
                        "file_size": log.file_size,
                        "records_count": log.records_count,
                        "download_duration": log.download_duration,
                        "parse_duration": log.parse_duration,
                        "status": log.status,
                        "error_message": log.error_message,
                        "started_at": log.started_at.isoformat() if log.started_at else None,
                        "completed_at": log.completed_at.isoformat() if log.completed_at else None,
                    }
                    for log in db_logs
                ]
            except Exception as e:
                logger.warning(f"Failed to get download logs: {e}")
        
        return jsonify({
            "logs": logs,
            "count": len(logs),
        })
    
    @app.route('/health')
    def health_check():
        """Health check endpoint."""
        return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})
    
    @app.route('/api/stats')
    def get_stats():
        """Get dashboard statistics."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        try:
            stats = app.repository.get_dashboard_stats()
            return jsonify(stats)
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/tlds')
    def get_tlds():
        """Get TLD statistics."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        try:
            tlds = app.repository.get_tld_stats()
            return jsonify({"tlds": tlds, "count": len(tlds)})
        except Exception as e:
            logger.error(f"Failed to get TLD stats: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/search')
    def search_domains():
        """Search domains."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        query = request.args.get('q', '')
        tld = request.args.get('tld', None)
        record_type = request.args.get('type', None)
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)
        
        if not query or len(query) < 2:
            return jsonify({"error": "Query must be at least 2 characters"}), 400
        
        try:
            offset = (page - 1) * per_page
            domains, total = app.repository.search_domains(
                query=query,
                tld=tld,
                record_type=record_type,
                limit=per_page,
                offset=offset
            )
            
            return jsonify({
                "domains": domains,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            })
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/available-tlds')
    def get_available_tlds():
        """Get list of available TLDs."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        try:
            tlds = app.repository.get_available_tlds()
            return jsonify({"tlds": tlds})
        except Exception as e:
            logger.error(f"Failed to get available TLDs: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/available-dates')
    def get_available_dates():
        """Get list of available download dates."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        tld = request.args.get('tld', None)
        
        try:
            dates = app.repository.get_available_dates(tld)
            return jsonify({"dates": dates})
        except Exception as e:
            logger.error(f"Failed to get available dates: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/dropped-domains')
    def get_dropped_domains():
        """Get domains that were dropped between two dates."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        tld = request.args.get('tld')
        old_date = request.args.get('old_date')
        new_date = request.args.get('new_date')
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 100, type=int), 1000)
        
        if not all([tld, old_date, new_date]):
            return jsonify({"error": "tld, old_date, and new_date are required"}), 400
        
        try:
            offset = (page - 1) * per_page
            domains, total = app.repository.get_dropped_domains(
                tld=tld,
                old_date=old_date,
                new_date=new_date,
                limit=per_page,
                offset=offset
            )
            
            return jsonify({
                "domains": domains,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page if total > 0 else 0,
                "tld": tld,
                "old_date": old_date,
                "new_date": new_date,
            })
        except Exception as e:
            logger.error(f"Failed to get dropped domains: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/new-domains')
    def get_new_domains():
        """Get domains that were newly registered between two dates."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        tld = request.args.get('tld')
        old_date = request.args.get('old_date')
        new_date = request.args.get('new_date')
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 100, type=int), 1000)
        
        if not all([tld, old_date, new_date]):
            return jsonify({"error": "tld, old_date, and new_date are required"}), 400
        
        try:
            offset = (page - 1) * per_page
            domains, total = app.repository.get_new_domains(
                tld=tld,
                old_date=old_date,
                new_date=new_date,
                limit=per_page,
                offset=offset
            )
            
            return jsonify({
                "domains": domains,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page if total > 0 else 0,
                "tld": tld,
                "old_date": old_date,
                "new_date": new_date,
            })
        except Exception as e:
            logger.error(f"Failed to get new domains: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/domain-changes')
    def get_domain_changes():
        """Get summary of domain changes between two dates."""
        if not app.repository:
            return jsonify({"error": "Repository not available"}), 503
        
        tld = request.args.get('tld')
        old_date = request.args.get('old_date')
        new_date = request.args.get('new_date')
        
        if not all([tld, old_date, new_date]):
            return jsonify({"error": "tld, old_date, and new_date are required"}), 400
        
        try:
            summary = app.repository.get_domain_changes_summary(
                tld=tld,
                old_date=old_date,
                new_date=new_date
            )
            return jsonify(summary)
        except Exception as e:
            logger.error(f"Failed to get domain changes: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/dropped')
    def dropped_page():
        """Render dropped domains page."""
        return render_template('dropped.html')
    
    @app.route('/browse')
    def browse_page():
        """Render domain browser page."""
        return render_template('browse.html')
    
    @app.route('/admin')
    def admin_page():
        """Render admin dashboard page."""
        return render_template('dashboard.html')
    
    # SocketIO Events
    @socketio.on('connect')
    def handle_connect():
        """Handle client connection."""
        logger.info("Client connected to WebSocket")
    
    @socketio.on('disconnect')
    def handle_disconnect():
        """Handle client disconnection."""
        logger.info("Client disconnected from WebSocket")
    
    @socketio.on('subscribe_logs')
    def handle_subscribe():
        """Subscribe client to real-time logs."""
        logger.info("Client subscribed to logs")
        # Send recent logs to newly connected client
        if app.logger_service:
            logs = app.logger_service.get_logs_as_dicts(50)
            socketio.emit('initial_logs', {'logs': logs})
    
    return app, socketio
