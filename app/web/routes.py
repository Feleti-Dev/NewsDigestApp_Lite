"""
Маршруты веб-интерфейса для Quart (асинхронный Flask).
Содержит все API endpoints для управления приложением.
"""
import asyncio
import logging
import os
from datetime import datetime

from quart import (
    Blueprint, render_template, jsonify,
    request
)
from quart_cors import cors

from app.configs import config
from app.database.db_utils import DatabaseManager
from app.database.models import SessionLocal
from app.parsers.parser_manager import ParserManager
from app.scheduler.digest_scheduler import DigestScheduler
from app.scheduler.scheduler_manager import SchedulerManager

logger = logging.getLogger(__name__)

bp = Blueprint('web', __name__, url_prefix='/')
cors(bp)  # Включаем cors для API


# Глобальные инстансы менеджеров
parser_manager: ParserManager = None
db_manager: DatabaseManager = None
digest_scheduler: DigestScheduler = None
scheduler_manager: SchedulerManager = None


def safe_get(dictionary, key, default=None):
    """Безопасное получение значения из словаря"""
    if dictionary is None or not isinstance(dictionary, dict):
        return default
    return dictionary.get(key, default)


def set_managers(
    out_parser_manager: ParserManager = None,
    out_db_manager: DatabaseManager = None,
    out_digest_scheduler: DigestScheduler = None,
    out_scheduler_manager: SchedulerManager = None,
):
    """Установка глобальных менеджеров"""
    global parser_manager, db_manager, digest_scheduler, scheduler_manager
    parser_manager = out_parser_manager
    db_manager = out_db_manager
    digest_scheduler = out_digest_scheduler
    scheduler_manager = out_scheduler_manager


# ========================
# СТРАНИЦЫ
# ========================

@bp.route('/')
async def index():
    """Главная страница"""
    return await render_template('index.html')

@bp.route('/dashboard')
async def dashboard():
    """Панель мониторинга"""
    return await render_template('index.html')

@bp.route('/logs')
async def logs_page():
    """Страница логов"""
    return await render_template('logs.html')

@bp.route('/tasks')
async def tasks_page():
    """Страница управления задачами"""
    return await render_template('tasks.html')

@bp.route('/news')
async def news_page():
    return await render_template('news.html')

@bp.route('/channels')
async def channels_page():
    """Страница каналов"""
    return await render_template('channels.html')

@bp.route('/settings')
async def settings_page():
    """Страница настроек"""
    return await render_template('settings.html')


# ========================
# API: КОНФИГУРАЦИЯ
# ========================

@bp.route('/api/config/settings')
async def config_settings():
    """Получение настроек из конфигурации"""
    try:
        from app.configs import config as app_config

        # Получаем интервалы из конфигурации
        intervals = {}
        if hasattr(app_config, 'scheduler') and hasattr(app_config.scheduler, 'intervals'):
            intervals = {
                source_type: f"{int(interval_seconds / 60)} минут" if interval_seconds >= 60
                else f"{interval_seconds} секунд"
                for source_type, interval_seconds in app_config.scheduler.intervals.items()
            }

        # Получаем расписание дайджестов
        digest_schedule = {}
        if hasattr(app_config, 'scheduler'):
            scheduler = app_config.scheduler
            # Ежедневный дайджест
            daily_time = "12:00 МСК"
            if hasattr(scheduler, 'daily_digest') and scheduler.daily_digest:
                daily_time = f"{scheduler.daily_digest.get('hour', 12):02d}:{scheduler.daily_digest.get('minute', 0):02d} МСК"

            # Еженедельный дайджест
            weekly_time = "Воскресенье 12:00 МСК"
            if hasattr(scheduler, 'weekly_digest') and scheduler.weekly_digest:
                weekday = scheduler.weekly_digest.get('day_of_week', 'sun')
                weekdays_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
                weekday_num = weekdays_map.get(weekday, 6)
                weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
                weekly_time = f"{weekdays[weekday_num]} {scheduler.weekly_digest.get('hour', 12):02d}:{scheduler.weekly_digest.get('minute', 0):02d} МСК"

            # Ежемесячный дайджест
            monthly_time = "28 число 12:00 МСК"
            if hasattr(scheduler, 'monthly_digest') and scheduler.monthly_digest:
                monthly_time = f"{scheduler.monthly_digest.get('day', 28)} число {scheduler.monthly_digest.get('hour', 12):02d}:{scheduler.monthly_digest.get('minute', 0):02d} МСК"

            digest_schedule = {
                'daily': daily_time,
                'weekly': weekly_time,
                'monthly': monthly_time
            }

        settings = {
            'intervals': intervals,
            'digest_schedule': digest_schedule,
            'timestamp': datetime.now().isoformat()
        }

        return jsonify(settings)

    except Exception as e:
        logger.error(f"Ошибка получения настроек конфигурации: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'intervals': {},
            'digest_schedule': {},
            'timestamp': datetime.now().isoformat()
        }), 500


# ========================
# API: НОВОСТИ
# ========================

@bp.route('/api/news')
async def api_news():
    """Получение списка новостей с пагинацией"""
    try:
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 20))
        sort_by = request.args.get("sort", "date")
        order = request.args.get("order", "desc")

        exclude_sources = request.args.getlist("exclude_sources[]")
        exclude_types = request.args.getlist("exclude_types[]")

        total, items = db_manager.get_news_paginated(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=order,
            exclude_sources=exclude_sources,
            exclude_source_types=exclude_types
        )

        data = [{
            "id": n.ID,
            "headline": n.Headline,
            "source": n.Source,
            "url": n.News_URL,
            "score": float(n.Interest_score or 0),
            "date": n.Publication_date.isoformat() if n.Publication_date else None,
            # ✅ Добавлены флаги использования
            "daily_used": n.Daily_used,
            "weekly_used": n.Weekly_used,
            "monthly_used": n.Monthly_used
        } for n in items]

        return jsonify({
            "items": data,
            "total": total,
            "page": page,
            "per_page": per_page
        })

    except Exception as e:
        logger.error(f"Ошибка api/news: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route('/api/news/<int:news_id>', methods=['DELETE'])
async def delete_news(news_id):
    """Удаление новости"""
    success = db_manager.delete_news(news_id)
    return jsonify({"success": success})


@bp.route('/api/news/recent')
async def recent_news():
    """Недавние новости"""
    try:
        # ✅ Используем метод из db_manager вместо прямого запроса
        news_data = db_manager.get_recent_news(hours=24, limit=20)

        return jsonify({
            'news': news_data,
            'total': len(news_data),
            'period': '24 часа'
        })

    except Exception as e:
        logger.error(f"Ошибка получения новостей: {e}")
        return jsonify({'error': str(e)}), 500


# ========================
# API: СИСТЕМА
# ========================

@bp.route('/api/system/status')
async def system_status():
    """Статус системы"""
    try:
        # Получаем статистику от scheduler_manager если он доступен
        scheduler_status = {}
        if scheduler_manager:
            try:
                all_statuses = scheduler_manager.get_all_statuses()
                scheduler_status = all_statuses
            except Exception as e:
                logger.error(f"Ошибка получения статуса scheduler: {e}")

        # Базовая статистика
        db_stats = {}
        if db_manager:
            db_stats = db_manager.get_stats() or {}

        # Получаем данные от parser_manager
        parser_summary = {}
        if parser_manager:
            try:
                parser_summary = parser_manager.get_parsers_status() or {}
            except Exception as e:
                logger.error(f"Ошибка получения сводки от парсера: {e}")
                parser_summary = {}

        # Формируем статус планировщиков
        schedulers_info = {
            'continuous': safe_get(scheduler_status, 'continuous', {}),
            'single_pass': safe_get(scheduler_status, 'single_pass', {}),
            'digest_publisher': safe_get(scheduler_status, 'digest_publisher', {}),
            'sheets_sync': safe_get(scheduler_status, 'sheets_sync', {})
        }

        status = {
            'database': {
                'connected': bool(db_stats and 'total_news' in db_stats),
                'news_count': safe_get(db_stats, 'total_news', 0),
                'news_with_score': safe_get(db_stats, 'news_with_score', 0),
                'average_score': safe_get(db_stats, 'average_score', 0.0),
                'news_last_24h': safe_get(db_stats, 'news_last_24h', 0),
                'path': safe_get(db_stats, 'database_path', 'Unknown')
            },
            'parsers': safe_get(parser_summary, 'api_status', {}),
            'schedulers': schedulers_info,
            'timestamp': safe_get(db_stats, 'timestamp', datetime.now().isoformat())
        }

        return jsonify(status)

    except Exception as e:
        logger.error(f"Ошибка получения статуса: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'database': {
                'connected': False,
                'news_count': 0,
                'news_with_score': 0,
                'average_score': 0.0,
                'news_last_24h': 0,
                'path': 'Error'
            }
        }), 500


@bp.route('/api/stats/detailed')
async def detailed_stats():
    """Детальная статистика"""
    try:
        # Получаем статистику от scheduler_manager
        scheduler_status = {}
        if scheduler_manager:
            try:
                scheduler_status = scheduler_manager.get_all_statuses()
            except Exception as e:
                logger.error(f"Ошибка получения статуса scheduler: {e}")

        # Полная статистика из базы данных
        db_stats = {}
        if db_manager:
            try:
                db_stats = db_manager.get_stats() or {}
            except Exception as e:
                logger.error(f"Ошибка получения статистики из БД: {e}")

        # Сводка парсеров
        parser_summary = {}
        if parser_manager:
            try:
                parser_summary = parser_manager.get_parsers_status() or {}
            except Exception as e:
                logger.error(f"Ошибка получения сводки от парсера: {e}")

        # ✅ ИСПРАВЛЕНИЕ: Статистика по источникам из БД каналов
        source_stats = []
        if db_manager:
            try:
                source_stats = db_manager.get_parser_statistics()
            except Exception as e:
                logger.error(f"Ошибка получения статистики по источникам: {e}")

        # Если пусто - fallback
        if not source_stats:
            for source_type in ['Telegram', 'Twitter', 'Youtube', 'Reddit']:
                source_stats.append({
                    'type': source_type,
                    'channels': 0,
                    'active_channels': 0,
                    'success_rate': 0.0,
                    'news_collected': 0
                })

        stats = {
            'database': {
                'total_news': safe_get(db_stats, 'total_news', 0),
                'news_with_score': safe_get(db_stats, 'news_with_score', 0),
                'source_stats': safe_get(db_stats, 'source_stats', {}),
                'daily_used': safe_get(db_stats, 'daily_used', 0),
                'weekly_used': safe_get(db_stats, 'weekly_used', 0),
                'monthly_used': safe_get(db_stats, 'monthly_used', 0),
                'total_channels': safe_get(db_stats, 'total_channels', 0),
                'active_channels': safe_get(db_stats, 'active_channels', 0),
                'news_last_24h': safe_get(db_stats, 'news_last_24h', 0),
                'news_last_7d': safe_get(db_stats, 'news_last_7d', 0),
                'news_last_30d': safe_get(db_stats, 'news_last_30d', 0),
                'average_score': safe_get(db_stats, 'average_score', 0.0)
            },
            'parsers': safe_get(parser_summary, 'api_status', {}),
            'sources': source_stats,
            'last_updated': safe_get(db_stats, 'timestamp', datetime.now().isoformat())
        }

        return jsonify(stats)

    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'database': {
                'total_news': 0,
                'news_with_score': 0,
                'source_stats': {},
                'daily_used': 0,
                'weekly_used': 0,
                'monthly_used': 0,
                'total_channels': 0,
                'active_channels': 0,
                'news_last_24h': 0,
                'news_last_7d': 0,
                'news_last_30d': 0,
                'average_score': 0.0
            },
            'parsers': {},
            'sources': [],
            'last_updated': datetime.now().isoformat()
        }), 500


# ========================
# API: ПЛАНИРОВЩИКИ
# ========================

@bp.route('/api/schedulers/status')
async def schedulers_status():
    """Получение статуса всех планировщиков"""
    try:
        if not scheduler_manager:
            return jsonify({
                'success': False,
                'error': 'SchedulerManager не инициализирован',
                'schedulers': {},
                'mutual_exclusion': {
                    'exclusive_group': ['continuous', 'single_pass'],
                    'description': 'Может быть активен только один из: цикличный или единичный сборщик',
                    'active_scheduler': None
                }
            }), 500

        # Получаем статусы всех планировщиков
        all_statuses = scheduler_manager.get_all_statuses()

        # Получаем статус mutual exclusion
        mutual_exclusion = scheduler_manager.get_mutual_exclusion_status()

        return jsonify({
            'success': True,
            'schedulers': all_statuses,
            'mutual_exclusion': mutual_exclusion,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Ошибка получения статуса планировщиков: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'schedulers': {},
            'mutual_exclusion': {
                'exclusive_group': ['continuous', 'single_pass'],
                'description': 'Может быть активен только один из: цикличный или единичный сборщик',
                'active_scheduler': None
            }
        }), 500


@bp.route('/api/schedulers/control', methods=['POST'])
async def schedulers_control():
    """Управление планировщиками.

    Использует background tasks Quart для запуска long-running операций.
    Планировщики запускаются в фоне, не блокируя ответ клиенту.
    """
    try:
        content_type = request.content_type
        if not content_type or 'application/json' not in content_type:
            return jsonify({
                'success': False,
                'message': 'Content-Type должен быть application/json'
            }), 415

        data = await request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'message': 'Нет данных JSON'
            }), 400

        scheduler_type = data.get('scheduler_type')
        action = data.get('action')

        if not scheduler_type or not action:
            return jsonify({
                'success': False,
                'message': 'Не указаны scheduler_type или action'
            }), 400

        if not scheduler_manager:
            return jsonify({
                'success': False,
                'message': 'SchedulerManager не инициализирован'
            }), 500

        # Выполняем действие
        result = {}

        if scheduler_type == 'continuous':
            if action == 'start':
                # Запускаем в background task - не блокируем ответ
                asyncio.create_task(_run_continuous_scheduler(scheduler_manager))
                return jsonify({
                    'success': True,
                    'message': 'Continuous scheduler запущен в фоне',
                    'scheduler_type': scheduler_type
                })
            elif action == 'stop':
                result = await scheduler_manager.stop_continuous()

        elif scheduler_type == 'single_pass':
            if action == 'start':
                # Запускаем в background task - используем встроенный механизм Quart
                asyncio.create_task(_run_single_pass_scheduler(scheduler_manager))
                return jsonify({
                    'success': True,
                    'message': 'Single pass scheduler запущен в фоне',
                    'scheduler_type': scheduler_type
                })
            elif action == 'stop':
                result = await scheduler_manager.stop_single_pass()

        elif scheduler_type == 'digest_publisher':
            if action == 'start':
                asyncio.create_task(_run_digest_publisher(scheduler_manager))
                return jsonify({
                    'success': True,
                    'message': 'Digest publisher запущен в фоне',
                    'scheduler_type': scheduler_type
                })
            elif action == 'stop':
                result = await scheduler_manager.stop_digest_publisher()

        elif scheduler_type == 'sheets_sync':
            if action == 'start':
                result = await scheduler_manager.start_sheets_sync()
            # sheets_sync нельзя остановить отдельно

        else:
            return jsonify({
                'success': False,
                'message': f'Неизвестный тип планировщика: {scheduler_type}'
            }), 400

        return jsonify({
            'success': True,
            'message': f'{scheduler_type} {action} completed',
            'scheduler': result
        })

    except ValueError as ve:
        # Ошибка mutual exclusion
        return jsonify({
            'success': False,
            'error': str(ve),
            'error_code': 'SCHEDULER_CONFLICT',
            'message': str(ve)
        }), 400

    except Exception as e:
        logger.error(f"Ошибка управления планировщиками: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500


# ========================
# Background Tasks для планировщиков
# ========================

async def _run_continuous_scheduler(scheduler_manager: SchedulerManager):
    """Background task для запуска continuous scheduler"""
    try:
        logger.info("🚀 Запуск continuous scheduler в background task...")
        await scheduler_manager.start_continuous()
    except Exception as e:
        logger.error(f"Ошибка в continuous scheduler: {e}")

async def _run_single_pass_scheduler(scheduler_manager: SchedulerManager):
    """Background task для запуска single pass scheduler.

    Это ключевое решение: Quart background tasks живут независимо от HTTP-запроса,
    event loop не закрывается после отправки ответа клиенту.
    """
    try:
        logger.info("🚀 Запуск single pass scheduler в background task...")
        await scheduler_manager.start_single_pass()
    except Exception as e:
        logger.error(f"Ошибка в single pass scheduler: {e}")

async def _run_digest_publisher(scheduler_manager: SchedulerManager):
    """Background task для запуска digest publisher"""
    try:
        logger.info("🚀 Запуск digest publisher в background task...")
        await scheduler_manager.start_digest_publisher()
    except Exception as e:
        logger.error(f"Ошибка в digest publisher: {e}")


@bp.route('/api/tasks/status')
async def tasks_status():
    """Статус задач"""
    try:
        if not scheduler_manager:
            return jsonify({
                'error': 'SchedulerManager не инициализирован',
                'continuous_scheduler': {
                    'running': False, 'start_time': None,
                    'uptime': None, 'tasks_running': 0,
                    'scheduler_type': 'unknown'
                },
                'digest_scheduler': {
                    'running': 'unknown', 'next_daily': '12:00 МСК',
                    'next_weekly': 'Воскресенье 12:00 МСК',
                    'next_monthly': '28 число 12:00 МСК'
                },
                'last_check': datetime.now().isoformat()
            }), 500

        all_statuses = scheduler_manager.get_all_statuses()
        continuous_status = safe_get(all_statuses, 'continuous', {})

        return jsonify({
            'continuous_scheduler': {
                'running': safe_get(continuous_status, 'running', False),
                'start_time': safe_get(continuous_status, 'start_time'),
                'uptime': safe_get(continuous_status, 'uptime'),
                'tasks_running': 0,
                'scheduler_type': safe_get(continuous_status, 'scheduler_type', 'unknown')
            },
            'digest_scheduler': {
                'running': 'unknown',
                'next_daily': '12:00 МСК',
                'next_weekly': 'Воскресенье 12:00 МСК',
                'next_monthly': '28 число 12:00 МСК'
            },
            'last_check': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Ошибка получения статуса задач: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'continuous_scheduler': {
                'running': False, 'start_time': None,
                'uptime': None, 'tasks_running': 0,
                'scheduler_type': 'unknown'
            },
            'digest_scheduler': {
                'running': 'error', 'next_daily': 'N/A',
                'next_weekly': 'N/A', 'next_monthly': 'N/A'
            },
            'last_check': datetime.now().isoformat()
        }), 500


@bp.route('/api/tasks/control', methods=['POST'])
async def control_task():
    """Управление задачами"""
    try:
        content_type = request.content_type
        if not content_type or 'application/json' not in content_type:
            return jsonify({
                'success': False,
                'message': 'Content-Type должен быть application/json'
            }), 415

        data = await request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'message': 'Нет данных JSON'
            }), 400

        task_type = data.get('task_type')
        action = data.get('action')

        if not task_type or not action:
            return jsonify({
                'success': False,
                'message': 'Не указаны task_type или action'
            }), 400

        logger.info(f"task_type: {task_type}, action: {action}")

        if task_type == 'continuous':
            if not scheduler_manager:
                return jsonify({
                    'success': False,
                    'message': 'SchedulerManager не инициализирован'
                }), 500

            if action == 'start':
                asyncio.create_task(_run_continuous_scheduler(scheduler_manager))
                return jsonify({
                    'success': True,
                    'message': 'Continuous scheduler запущен'
                })
            elif action == 'stop':
                await scheduler_manager.stop_continuous()
                return jsonify({
                    'success': True,
                    'message': 'Continuous scheduler остановлен'
                })

        elif task_type == 'digest':
            digest_type = data.get('digest_type', 'daily')
            if action == 'force_execute':
                if not digest_scheduler or not digest_scheduler.telegram_publisher:
                    return jsonify({
                        'success': False,
                        'message': 'DigestScheduler или Telegram_Publisher не инициализирован'
                    }), 500

                # Запускаем дайджест как background task
                asyncio.create_task(_force_execute_digest(digest_scheduler, digest_type))
                return jsonify({
                    'success': True,
                    'message': f'Запрос на дайджест {digest_type} запущен'
                })

        elif task_type == 'sync':
            if action == 'force':
                if not scheduler_manager:
                    return jsonify({
                        'success': False,
                        'message': 'SchedulerManager не инициализирован'
                    }), 500

                # Запускаем синхронизацию как background task
                asyncio.create_task(_force_sheets_sync(scheduler_manager))
                return jsonify({
                    'success': True,
                    'message': 'Синхронизация запущена'
                })

        return jsonify({
            'success': False,
            'message': f'Неизвестная команда: {task_type}/{action}'
        }), 400

    except Exception as e:
        logger.error(f"Ошибка управления задачами: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500


# ========================
# Дополнительные Background Tasks
# ========================

async def _force_execute_digest(digest_scheduler: DigestScheduler, digest_type: str):
    """Background task для принудительного выполнения дайджеста"""
    try:
        await digest_scheduler.force_execute_digest(digest_type)
    except Exception as e:
        logger.error(f"Ошибка принудительного выполнения дайджеста: {e}")

async def _force_sheets_sync(scheduler_manager: SchedulerManager):
    """Background task для принудительной синхронизации с Google Sheets"""
    try:
        await scheduler_manager.start_sheets_sync()
    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}")


# ========================
# API: НАСТРОЙКИ
# ========================

@bp.route('/api/settings/api')
async def settings_api():
    """Получение API настроек для редактирования"""
    try:
        api_config = {
            'telegram_bot_token': config.api.telegram_bot_token,
            'telegram_channel_id': config.api.telegram_channel_id,
            'telegram_api_id': config.api.telegram_api_id,
            'telegram_api_hash': config.api.telegram_api_hash,
            'telegram_2fa_password': config.api.telegram_2fa_password,
            'telegram_phone': config.api.telegram_phone,
            'twitter_bearer_token': config.api.twitter_bearer_token,
            'youtube_api_key': config.api.youtube_api_key,
            'groq_api_key': config.api.groq_api_key,
            'google_spreadsheet_id': config.google_sheets.google_spreadsheet_id,
        }

        return jsonify({
            'success': True,
            'api': api_config,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Ошибка получения API настроек: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'api': {}
        }), 500

@bp.route('/api/settings/config')
async def settings_config():
    """Получение конфигурации для редактирования"""
    try:
        from app.configs import config as app_config

        # Основные настройки
        app_settings = {
            'bypassing_method': app_config.app.bypassing_method,
            'interest_threshold': app_config.app.interest_threshold,
            'max_news_per_channel': app_config.app.max_news_per_channel,
            'max_news_per_digest': app_config.app.max_news_per_digest,
            'max_news_time_period': app_config.app.max_news_time_period,
            'model_type': app_config.app.model_type,
            'topic': app_config.app.topic
        }

        # Интервалы - конвертируем timedelta в секунды
        intervals = {}
        if hasattr(app_config.scheduler, 'intervals') and app_config.scheduler.intervals:
            for source_type, interval_value in app_config.scheduler.intervals.items():
                if hasattr(interval_value, 'total_seconds'):
                    intervals[source_type] = int(interval_value.total_seconds())
                elif isinstance(interval_value, (int, float)):
                    intervals[source_type] = int(interval_value)
                else:
                    intervals[source_type] = interval_value

        # Статусы парсеров
        parser_status = {
            'twitter': app_config.app.parser_status.get('twitter', False),
            'telegram': app_config.app.parser_status.get('telegram', False),
            'youtube': app_config.app.parser_status.get('youtube', False)
        }

        # Расписание дайджестов
        digest_schedule = {
            'daily': app_config.scheduler.daily_digest,
            'weekly': app_config.scheduler.weekly_digest,
            'monthly': app_config.scheduler.monthly_digest
        }

        parser_schedule = {
            'daily_single_pass': app_config.scheduler.daily_single_pass,
        }

        return jsonify({
            'success': True,
            'config': {
                'app': app_settings,
                'intervals': intervals,
                'parser_status': parser_status,
                'digest_schedule': digest_schedule,
                'parser_schedule': parser_schedule,
            },
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Ошибка получения конфигурации: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'config': {}
        }), 500

@bp.route('/api/settings/prompts')
async def settings_prompts():
    """Получение промптов для редактирования"""
    try:
        from app.configs.llm_prompts import get_all_prompts

        prompts = get_all_prompts()

        return jsonify({
            'success': True,
            'prompts': prompts,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Ошибка получения промптов: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'prompts': {}
        }), 500

@bp.route('/api/settings/save', methods=['POST'])
async def settings_save():
    """Сохранение настроек с обновлением конфигурации"""
    try:
        content_type = request.content_type
        if not content_type or 'application/json' not in content_type:
            return jsonify({
                'success': False,
                'message': 'Content-Type должен быть application/json'
            }), 415

        data = await request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'message': 'Нет данных JSON'
            }), 400

        config_updates = data.get('config_updates', {})
        prompts_updates = data.get('prompts_updates', {})
        section = data.get('section', 'app')

        # Сохранение API настроек
        if section == 'api' and config_updates:
            from app.configs.env_manager import EnvManager
            env_manager = EnvManager()
            if not env_manager.save_and_update_config(section, config_updates, config):
                return jsonify({
                    'success': False,
                    'message': 'Ошибка сохранения API настроек'
                }), 500
            logger.info(f"Сохранено {len(config_updates)} API параметров")

        # Сохранение конфигурации в .env и обновление config
        elif config_updates:
            from app.configs.env_manager import EnvManager
            env_manager = EnvManager()
            if not env_manager.save_and_update_config(section, config_updates, config):
                return jsonify({
                    'success': False,
                    'message': 'Ошибка сохранения конфигурации'
                }), 500
            logger.info(f"Сохранено {len(config_updates)} параметров конфигурации")

            # Если это настройки планировщика - перезапускаем digest_scheduler
            if section == 'scheduler' and scheduler_manager:
                await scheduler_manager.restart_digest_scheduler()
                logger.info("DigestScheduler перезапущен с новыми настройками")

            # Если это настройки парсеров - перезапускаем парсеры
            if section == 'parsers' and parser_manager:
                # Проверяем, не запущен ли парсинг новостей
                if scheduler_manager and await scheduler_manager.is_news_parsing_running():
                    return jsonify({
                        'success': False,
                        'message': 'Нельзя изменить настройки парсеров во время работы парсинга новостей. Остановите сначала Continuous или Single Pass scheduler.'
                    }), 400
                await parser_manager.restart_parsers()
                logger.info("Парсеры перезапущены с новыми настройками")

        # Сохранение промптов с немедленным обновлением переменных
        if prompts_updates:
            from app.configs.llm_prompts import save_prompts
            if not save_prompts(prompts_updates):
                return jsonify({
                    'success': False,
                    'message': 'Ошибка сохранения промптов'
                }), 500
            logger.info(f"Сохранено {len(prompts_updates)} промптов с немедленным обновлением")

        return jsonify({
            'success': True,
            'message': 'Настройки сохранены'
        })

    except Exception as e:
        logger.error(f"Ошибка сохранения настроек: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500


# ========================
# API: КАНАЛЫ
# ========================

@bp.route('/api/channels/list')
async def channels_list():
    """Список каналов"""
    session = None
    try:
        from app.database.models import ChannelSource
        from sqlalchemy import desc

        channels = []
        session = SessionLocal()
        db_channels = session.query(ChannelSource).order_by(
            desc(ChannelSource.id)
        ).all()

        for channel in db_channels:
            channels.append({
                'source': channel.source_type,
                'url': channel.url,
                'channel_id': channel.channel_id,
                'is_active': channel.is_active,
                'news_collected': channel.news_collected,
                'avg_interest_score': channel.avg_interest_score,
                'success_count': channel.success_count,
                'failure_count': channel.failure_count,
                'last_success': channel.last_checked.isoformat() if channel.last_checked else None,
                'last_processed': channel.last_processed.isoformat() if channel.last_processed else None,
                'deactivation_reason': None if channel.is_active else 'Из базы данных'
            })

    except Exception as e:
        logger.error(f"Ошибка получения каналов из БД: {e}")
        return jsonify({
            'error': str(e),
            'channels': [],
            'total': 0,
            'active': 0
        }), 500
    finally:
        session.close()

    return jsonify({
        'channels': channels,
        'total': len(channels),
        'active': len([c for c in channels if c.get('is_active', False)])
    })


# ========================
# API: ЛОГИ
# ========================

def _get_log_files_list():
    """Вспомогательная функция для получения списка лог-файлов"""
    files = []
    if os.path.exists(config.app.logs_dir):
        # Получаем все файлы .log в директории
        all_files = [f for f in os.listdir(config.app.logs_dir) if f.endswith('.log')]
        # Сортируем: сначала свежие по дате изменения
        all_files.sort(key=lambda x: os.path.getmtime(os.path.join(config.app.logs_dir, x)), reverse=True)
        files = [{'name': f, 'id': i} for i, f in enumerate(all_files)]
    return files


@bp.route('/api/logs/files')
async def list_log_files():
    """Получение списка доступных файлов логов"""
    try:
        files = _get_log_files_list()
        return jsonify({'files': files})
    except Exception as e:
        logger.error(f"Ошибка получения списка файлов логов: {e}")
        return jsonify({'files': []}), 500


@bp.route('/api/logs/file')
async def get_log_content():
    """
    Получение логов из конкретного файла с пагинацией.
    Параметры:
      - log_file_index: индекс файла из списка (0 - самый свежий)
      - page: номер страницы (с 1)
      - per_page: строк на странице (по умолчанию 100)
    """
    try:
        files = _get_log_files_list()
        file_index = int(request.args.get('log_file_index', 0))
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))

        if not files or file_index >= len(files):
            return jsonify({
                'lines': [],
                'has_more': False,
                'file_name': 'Not found',
                'total_lines': 0
            })

        target_file = files[file_index]['name']
        file_path = os.path.join(config.app.logs_dir, target_file)

        lines = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Читаем все строки
                lines = f.readlines()
        except Exception as e:
            logger.error(f"Ошибка чтения файла {target_file}: {e}")
            return jsonify({'error': 'Ошибка чтения файла'}), 500

        total_lines = len(lines)

        # Разворачиваем, чтобы новые были сверху
        lines.reverse()

        # Пагинация
        start = (page - 1) * per_page
        end = start + per_page

        sliced_lines = lines[start:end]
        has_more = end < total_lines

        return jsonify({
            'lines': sliced_lines,
            'has_more': has_more,
            'file_name': target_file,
            'total_lines': total_lines,
            'page': page
        })

    except Exception as e:
        logger.error(f"Ошибка обработки запроса логов: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ========================
# API: УТИЛИТЫ
# ========================

@bp.route('/health')
async def health_check():
    """Проверка здоровья приложения"""
    try:
        db_stats = db_manager.get_stats()
        db_ok = db_stats is not None and 'total_news' in db_stats

        return jsonify({
            'status': 'healthy' if db_ok else 'degraded',
            'database': 'ok' if db_ok else 'error',
            'news_count': safe_get(db_stats, 'total_news', 0),
            'timestamp': safe_get(db_stats, 'timestamp', datetime.now().isoformat())
        })

    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@bp.route('/debug')
async def debug_page():
    """Страница отладки"""
    return await render_template('index.html')
