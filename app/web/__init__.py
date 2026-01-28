"""
Инициализация Flask приложения
"""
import logging
import os

from flask import Flask, jsonify
from flask_cors import CORS

from app.parsers import ParserManager
from app.scheduler.digest_scheduler import DigestScheduler

logger = logging.getLogger(__name__)

def create_app(digest_scheduler: DigestScheduler, parser_manager: ParserManager,db_manager=None, scheduler_manager=None, restart_manager=None):
    """
    Создание и конфигурация Flask приложения

    Args:
        digest_scheduler: экземпляр DigestScheduler
        parser_manager: экземпляр ParserManager
        scheduler_manager: экземпляр SchedulerManager (опционально)
        restart_manager: экземпляр RestartManager (опционально)

    Returns:
        Настроенный Flask Blueprint
    """
    app = Flask(__name__,
                template_folder='templates',
                static_folder='static')
    CORS(app)

    # Конфигурация
    # app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    # Обработчик ошибок 500
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(error) if app.config['DEBUG'] else 'An internal error occurred'
        }), 500

    # Обработчик ошибок 404
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({
            'error': 'Not Found',
            'message': 'The requested resource was not found'
        }), 404

    try:
        # Регистрация маршрутов
        logger.info("Регистрация маршрутов веб-интерфейса...")
        from . import routes
        # Устанавливаем менеджеры
        routes.set_managers(
            out_parser_manager=parser_manager,
            out_db_manager=db_manager,
            out_digest_scheduler=digest_scheduler,
            out_scheduler_manager=scheduler_manager,
            out_restart_manager=restart_manager
        )
        app.register_blueprint(routes.bp)
        logger.info("✅ Маршруты зарегистрированы")

    except Exception as e:
        logger.error(f"❌ Ошибка регистрации маршрутов: {e}", exc_info=True)


    return app