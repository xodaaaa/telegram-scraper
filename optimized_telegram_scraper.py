import os
import sqlite3
import json
import csv
import asyncio
import configparser
import gc
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List, Tuple, Any
from collections import defaultdict
from functools import wraps
from datetime import datetime
import logging
import sys

import aiohttp
import psutil
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, User, PeerChannel
from telethon.errors import FloodWaitError, RPCError
from tqdm import tqdm

# Configuración
@dataclass
class ScraperConfig:
    api_id: int
    api_hash: str
    phone: str
    max_concurrent_downloads: int = 15
    batch_size: int = 100
    max_retries: int = 5
    memory_limit_mb: int = 1024
    download_timeout: int = 300
    base_retry_delay: int = 1
    enable_media_scraping: bool = True
    
    @classmethod
    def from_file(cls, config_path: str = 'config.ini'):
        if os.path.exists(config_path):
            config = configparser.ConfigParser()
            config.read(config_path)
            
            return cls(
                api_id=config.getint('telegram', 'api_id'),
                api_hash=config.get('telegram', 'api_hash'),
                phone=config.get('telegram', 'phone'),
                max_concurrent_downloads=config.getint('performance', 'max_concurrent_downloads', fallback=15),
                batch_size=config.getint('performance', 'batch_size', fallback=100),
                max_retries=config.getint('performance', 'max_retries', fallback=5),
                memory_limit_mb=config.getint('performance', 'memory_limit_mb', fallback=1024),
                download_timeout=config.getint('performance', 'download_timeout', fallback=300),
                base_retry_delay=config.getint('performance', 'base_retry_delay', fallback=1),
                enable_media_scraping=config.getboolean('telegram', 'enable_media_scraping', fallback=True)
            )
        return cls(0, "", "")
    
    def save_to_file(self, config_path: str = 'config.ini'):
        config = configparser.ConfigParser()
        config['telegram'] = {
            'api_id': str(self.api_id),
            'api_hash': self.api_hash,
            'phone': self.phone,
            'enable_media_scraping': str(self.enable_media_scraping)
        }
        config['performance'] = {
            'max_concurrent_downloads': str(self.max_concurrent_downloads),
            'batch_size': str(self.batch_size),
            'max_retries': str(self.max_retries),
            'memory_limit_mb': str(self.memory_limit_mb),
            'download_timeout': str(self.download_timeout),
            'base_retry_delay': str(self.base_retry_delay)
        }
        
        with open(config_path, 'w') as f:
            config.write(f)

# Gestión de métricas
class MetricsCollector:
    def __init__(self):
        self.metrics = defaultdict(int)
        self.start_time = time.time()
        self.last_stats_time = time.time()
    
    def increment(self, metric_name: str, value: int = 1):
        self.metrics[metric_name] += value
    
    def get_stats(self) -> Dict[str, Any]:
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            elapsed = 1
        
        return {
            'elapsed_seconds': elapsed,
            'messages_per_second': self.metrics['messages_processed'] / elapsed,
            'downloads_per_second': self.metrics['media_downloaded'] / elapsed,
            'error_rate': self.metrics['errors'] / max(1, self.metrics['total_operations']),
            'total_messages': self.metrics['messages_processed'],
            'total_media': self.metrics['media_downloaded'],
            'total_errors': self.metrics['errors']
        }
    
    def print_stats(self):
        stats = self.get_stats()
        print(f"\n📊 Estadísticas:")
        print(f"⏱️  Tiempo transcurrido: {stats['elapsed_seconds']:.2f}s")
        print(f"📝 Mensajes procesados: {stats['total_messages']}")
        print(f"📷 Media descargada: {stats['total_media']}")
        print(f"⚡ Velocidad mensajes: {stats['messages_per_second']:.2f}/s")
        print(f"🔽 Velocidad descargas: {stats['downloads_per_second']:.2f}/s")
        print(f"❌ Tasa de errores: {stats['error_rate']:.2%}")

# Gestión de memoria
class MemoryManager:
    def __init__(self, max_memory_mb: int = 1024):
        self.max_memory_mb = max_memory_mb
        self.last_check = time.time()
        self.check_interval = 30  # Verificar cada 30 segundos
    
    def check_memory_usage(self) -> bool:
        current_time = time.time()
        if current_time - self.last_check < self.check_interval:
            return False
        
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > self.max_memory_mb:
                logging.warning(f"Uso de memoria alto: {memory_mb:.2f}MB")
                gc.collect()
                self.last_check = current_time
                return True
        except Exception as e:
            logging.error(f"Error verificando memoria: {e}")
        
        self.last_check = current_time
        return False

# Gestión de base de datos optimizada
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()
    
    def connect(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA cache_size=10000')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.create_tables()
        self.create_indexes()
    
    def create_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER UNIQUE,
                date TEXT,
                sender_id INTEGER,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                message TEXT,
                media_type TEXT,
                media_path TEXT,
                reply_to INTEGER,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def create_indexes(self):
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_message_id ON messages(message_id)',
            'CREATE INDEX IF NOT EXISTS idx_date ON messages(date)',
            'CREATE INDEX IF NOT EXISTS idx_sender_id ON messages(sender_id)',
            'CREATE INDEX IF NOT EXISTS idx_media_type ON messages(media_type)',
            'CREATE INDEX IF NOT EXISTS idx_processed_at ON messages(processed_at)'
        ]
        
        for index in indexes:
            self.conn.execute(index)
        self.conn.commit()
    
    def bulk_insert_messages(self, messages_data: List[Tuple]):
        if not messages_data:
            return
        
        try:
            self.conn.executemany('''
                INSERT OR REPLACE INTO messages 
                (message_id, date, sender_id, first_name, last_name, username, 
                 message, media_type, media_path, reply_to)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', messages_data)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error insertando mensajes: {e}")
            self.conn.rollback()
    
    def update_media_path(self, message_id: int, media_path: str):
        try:
            self.conn.execute(
                'UPDATE messages SET media_path = ? WHERE message_id = ?',
                (media_path, message_id)
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error actualizando media path: {e}")
    
    def get_messages_without_media(self) -> List[int]:
        cursor = self.conn.execute(
            'SELECT message_id FROM messages WHERE media_type IS NOT NULL AND media_path IS NULL'
        )
        return [row[0] for row in cursor.fetchall()]
    
    def close(self):
        if self.conn:
            self.conn.close()

# Decorador para reintentos con backoff
def retry_with_backoff(max_retries: int = 5, base_delay: int = 1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except FloodWaitError as e:
                    wait_time = e.seconds + 5
                    logging.warning(f"FloodWaitError: esperando {wait_time}s")
                    await asyncio.sleep(wait_time)
                except (TimeoutError, aiohttp.ClientError, RPCError) as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Falló después de {max_retries} intentos: {e}")
                        raise
                    
                    wait_time = base_delay * (2 ** attempt)
                    logging.warning(f"Intento {attempt + 1} fallido. Esperando {wait_time}s. Error: {e}")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logging.error(f"Error inesperado en intento {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(base_delay * (2 ** attempt))
            
            return None
        return wrapper
    return decorator

# Clase principal del scraper
class TelegramScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.client = TelegramClient('session', config.api_id, config.api_hash)
        self.state = self.load_state()
        self.metrics = MetricsCollector()
        self.memory_manager = MemoryManager(config.memory_limit_mb)
        self.user_cache = {}
        self.continuous_scraping_active = False
        
        # Configurar logging
        self.setup_logging()
        
        # Semáforo para descargas
        self.download_semaphore = asyncio.Semaphore(config.max_concurrent_downloads)
    
    def setup_logging(self):
        log_folder = 'logs'
        os.makedirs(log_folder, exist_ok=True)
        
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_folder, f"telegram_scraper_{current_time}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def display_ascii_art(self):
        WHITE = "\033[97m"
        RESET = "\033[0m"
        art = r"""
    ___________________  _________
    \__    ___/  _____/ /   _____/
      |    | /   \  ___ \_____  \ 
      |    | \    \_\  \/        \
      |____|  \______  /_______  /
                     \/        \/
        TELEGRAM SCRAPER OPTIMIZADO v2.0
        """
        print(WHITE + art + RESET)
    
    def load_state(self) -> Dict:
        state_file = 'state.json'
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error cargando estado: {e}")
        
        return {'channels': {}}
    
    def save_state(self):
        try:
            with open('state.json', 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logging.error(f"Error guardando estado: {e}")
    
    async def get_cached_sender(self, message) -> Optional[User]:
        """Obtiene información del sender con caché"""
        if message.sender_id in self.user_cache:
            return self.user_cache[message.sender_id]
        
        try:
            sender = await message.get_sender()
            if isinstance(sender, User):
                self.user_cache[message.sender_id] = sender
            return sender
        except Exception as e:
            logging.error(f"Error obteniendo sender: {e}")
            return None
    
    def process_message_data(self, message, sender) -> Tuple:
        """Procesa los datos del mensaje para inserción en DB"""
        return (
            message.id,
            message.date.strftime('%Y-%m-%d %H:%M:%S'),
            message.sender_id,
            getattr(sender, 'first_name', None) if isinstance(sender, User) else None,
            getattr(sender, 'last_name', None) if isinstance(sender, User) else None,
            getattr(sender, 'username', None) if isinstance(sender, User) else None,
            message.message,
            message.media.__class__.__name__ if message.media else None,
            None,  # media_path se actualiza después
            message.reply_to_msg_id if message.reply_to else None
        )
    
    @retry_with_backoff()
    async def download_media_with_retry(self, channel: str, message, db_manager: DatabaseManager):
        """Descarga media con reintentos y gestión de errores"""
        async with self.download_semaphore:
            if not message.media or not self.config.enable_media_scraping:
                return None
            
            channel_dir = os.path.join(os.getcwd(), channel)
            media_folder = os.path.join(channel_dir, 'media')
            os.makedirs(media_folder, exist_ok=True)
            
            # Determinar nombre del archivo
            media_file_name = None
            if isinstance(message.media, MessageMediaPhoto):
                media_file_name = f"{message.id}.jpg"
            elif isinstance(message.media, MessageMediaDocument):
                ext = getattr(message.file, 'ext', None) or 'bin'
                media_file_name = getattr(message.file, 'name', None) or f"{message.id}.{ext}"
            
            if not media_file_name:
                logging.warning(f"No se pudo determinar nombre de archivo para mensaje {message.id}")
                return None
            
            media_path = os.path.join(media_folder, media_file_name)
            
            # Verificar si ya existe
            if os.path.exists(media_path):
                logging.debug(f"Media ya existe: {media_path}")
                db_manager.update_media_path(message.id, media_path)
                return media_path
            
            # Descargar
            try:
                downloaded_path = await asyncio.wait_for(
                    message.download_media(file=media_folder),
                    timeout=self.config.download_timeout
                )
                
                if downloaded_path:
                    logging.info(f"Media descargada: {downloaded_path}")
                    db_manager.update_media_path(message.id, downloaded_path)
                    self.metrics.increment('media_downloaded')
                    return downloaded_path
                    
            except asyncio.TimeoutError:
                logging.error(f"Timeout descargando media del mensaje {message.id}")
                self.metrics.increment('download_timeouts')
            except Exception as e:
                logging.error(f"Error descargando media del mensaje {message.id}: {e}")
                self.metrics.increment('download_errors')
            
            self.metrics.increment('errors')
            return None
    
    async def media_download_worker(self, queue: asyncio.Queue, channel: str, db_manager: DatabaseManager):
        """Worker para procesar descargas de media"""
        while True:
            try:
                message = await queue.get()
                if message is None:  # Señal de fin
                    break
                
                await self.download_media_with_retry(channel, message, db_manager)
                queue.task_done()
                
            except Exception as e:
                logging.error(f"Error en worker de descarga: {e}")
                self.metrics.increment('errors')
    
    async def scrape_channel(self, channel: str, offset_id: int = 0) -> bool:
        """Scraping optimizado de canal"""
        try:
            # Obtener entidad
            if channel.startswith('-'):
                entity = await self.client.get_entity(PeerChannel(int(channel)))
            else:
                entity = await self.client.get_entity(channel)
            
            logging.info(f"Iniciando scraping del canal: {channel}")
            
            # Preparar base de datos
            channel_dir = os.path.join(os.getcwd(), channel)
            os.makedirs(channel_dir, exist_ok=True)
            db_path = os.path.join(channel_dir, f'{channel}.db')
            db_manager = DatabaseManager(db_path)
            
            # Cola para descargas de media
            media_queue = asyncio.Queue()
            
            # Iniciar workers de descarga
            workers = []
            for _ in range(min(5, self.config.max_concurrent_downloads)):
                worker = asyncio.create_task(
                    self.media_download_worker(media_queue, channel, db_manager)
                )
                workers.append(worker)
            
            # Obtener total de mensajes
            result = await self.client.get_messages(entity, offset_id=offset_id, reverse=True, limit=0)
            total_messages = result.total
            
            if total_messages == 0:
                print(f"No hay mensajes en el canal {channel}")
                return True
            
            # Procesar mensajes
            message_batch = []
            processed_count = 0
            last_message_id = offset_id
            
            with tqdm(total=total_messages, desc=f"Scraping {channel}", unit="msg") as pbar:
                async for message in self.client.iter_messages(entity, offset_id=offset_id, reverse=True):
                    try:
                        # Obtener sender
                        sender = await self.get_cached_sender(message)
                        
                        # Procesar mensaje
                        message_data = self.process_message_data(message, sender)
                        message_batch.append(message_data)
                        
                        # Encolar media para descarga
                        if message.media and self.config.enable_media_scraping:
                            await media_queue.put(message)
                        
                        processed_count += 1
                        last_message_id = message.id
                        self.metrics.increment('messages_processed')
                        self.metrics.increment('total_operations')
                        
                        # Insertar batch cuando esté lleno
                        if len(message_batch) >= self.config.batch_size:
                            db_manager.bulk_insert_messages(message_batch)
                            message_batch.clear()
                            
                            # Actualizar estado
                            self.state['channels'][channel] = last_message_id
                            self.save_state()
                        
                        pbar.update(1)
                        
                        # Verificar memoria
                        if self.memory_manager.check_memory_usage():
                            await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logging.error(f"Error procesando mensaje {message.id}: {e}")
                        self.metrics.increment('errors')
                        self.metrics.increment('total_operations')
            
            # Insertar mensajes restantes
            if message_batch:
                db_manager.bulk_insert_messages(message_batch)
                self.state['channels'][channel] = last_message_id
                self.save_state()
            
            # Finalizar workers de descarga
            for _ in workers:
                await media_queue.put(None)
            
            await asyncio.gather(*workers, return_exceptions=True)
            
            db_manager.close()
            logging.info(f"Scraping completado para canal {channel}")
            return True
            
        except Exception as e:
            logging.error(f"Error en scraping del canal {channel}: {e}")
            self.metrics.increment('errors')
            return False
    
    async def continuous_scraping(self):
        """Scraping continuo con intervalo"""
        self.continuous_scraping_active = True
        
        try:
            while self.continuous_scraping_active:
                print(f"\n🔄 Verificando nuevos mensajes...")
                
                for channel in self.state['channels']:
                    if not self.continuous_scraping_active:
                        break
                    
                    print(f"📡 Verificando canal: {channel}")
                    await self.scrape_channel(channel, self.state['channels'][channel])
                
                self.metrics.print_stats()
                
                if self.continuous_scraping_active:
                    print("⏸️  Esperando 60 segundos antes de la próxima verificación...")
                    await asyncio.sleep(60)
                
        except asyncio.CancelledError:
            print("🛑 Scraping continuo detenido")
        finally:
            self.continuous_scraping_active = False
    
    async def export_data(self):
        """Exporta datos a CSV y JSON"""
        for channel in self.state['channels']:
            if self.state['channels'][channel] == 0:
                print(f"No hay mensajes para exportar del canal {channel}")
                continue
            
            print(f"📤 Exportando datos del canal {channel}...")
            self.export_to_csv(channel)
            self.export_to_json(channel)
    
    def export_to_csv(self, channel: str):
        """Exporta mensajes a CSV"""
        try:
            db_path = os.path.join(channel, f'{channel}.db')
            csv_path = os.path.join(channel, f'{channel}.csv')
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM messages ORDER BY date')
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in cursor.description])
                writer.writerows(cursor.fetchall())
            
            conn.close()
            print(f"✅ CSV exportado: {csv_path}")
            
        except Exception as e:
            logging.error(f"Error exportando CSV para {channel}: {e}")
    
    def export_to_json(self, channel: str):
        """Exporta mensajes a JSON"""
        try:
            db_path = os.path.join(channel, f'{channel}.db')
            json_path = os.path.join(channel, f'{channel}.json')
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM messages ORDER BY date')
            
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            conn.close()
            print(f"✅ JSON exportado: {json_path}")
            
        except Exception as e:
            logging.error(f"Error exportando JSON para {channel}: {e}")
    
    async def list_channels(self):
        """Lista canales disponibles en la cuenta"""
        try:
            print("\n📺 Canales disponibles en tu cuenta:")
            async for dialog in self.client.iter_dialogs():
                if dialog.id != 777000:  # Excluir Telegram notifications
                    print(f"• {dialog.title} (ID: {dialog.id})")
        except Exception as e:
            logging.error(f"Error listando canales: {e}")
    
    async def view_channels(self):
        """Muestra canales guardados"""
        if not self.state['channels']:
            print("No hay canales guardados")
            return
        
        print("\n📋 Canales guardados:")
        for channel, last_id in self.state['channels'].items():
            print(f"• Canal: {channel} | Último mensaje: {last_id}")
    
    async def setup_credentials(self):
        """Configurar credenciales si no existen"""
        if not self.config.api_id or not self.config.api_hash or not self.config.phone:
            print("🔐 Configurando credenciales...")
            self.config.api_id = int(input("API ID: "))
            self.config.api_hash = input("API Hash: ")
            self.config.phone = input("Número de teléfono: ")
            self.config.save_to_file()
            print("✅ Credenciales guardadas")
    
    async def main_menu(self):
        """Menú principal"""
        while True:
            print("\n" + "="*50)
            print("🤖 TELEGRAM SCRAPER OPTIMIZADO")
            print("="*50)
            print("[A] Agregar canal")
            print("[R] Remover canal")
            print("[S] Scraping de todos los canales")
            print("[M] Toggle scraping de media (actualmente: {})".format(
                "activado" if self.config.enable_media_scraping else "desactivado"))
            print("[C] Scraping continuo")
            print("[E] Exportar datos")
            print("[V] Ver canales guardados")
            print("[L] Listar canales de la cuenta")
            print("[T] Mostrar estadísticas")
            print("[Q] Salir")
            print("="*50)
            
            choice = input("Selecciona una opción: ").lower().strip()
            
            try:
                if choice == 'a':
                    channel = input("ID del canal: ").strip()
                    self.state['channels'][channel] = 0
                    self.save_state()
                    print(f"✅ Canal {channel} agregado")
                
                elif choice == 'r':
                    channel = input("ID del canal a remover: ").strip()
                    if channel in self.state['channels']:
                        del self.state['channels'][channel]
                        self.save_state()
                        print(f"✅ Canal {channel} removido")
                    else:
                        print(f"❌ Canal {channel} no encontrado")
                
                elif choice == 's':
                    print("🚀 Iniciando scraping de todos los canales...")
                    for channel in self.state['channels']:
                        await self.scrape_channel(channel, self.state['channels'][channel])
                    self.metrics.print_stats()
                
                elif choice == 'm':
                    self.config.enable_media_scraping = not self.config.enable_media_scraping
                    self.config.save_to_file()
                    print(f"✅ Scraping de media {'activado' if self.config.enable_media_scraping else 'desactivado'}")
                
                elif choice == 'c':
                    print("🔄 Iniciando scraping continuo...")
                    print("Presiona Ctrl+C para detener")
                    task = asyncio.create_task(self.continuous_scraping())
                    try:
                        await task
                    except KeyboardInterrupt:
                        print("\n🛑 Deteniendo scraping continuo...")
                        self.continuous_scraping_active = False
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                
                elif choice == 'e':
                    await self.export_data()
                
                elif choice == 'v':
                    await self.view_channels()
                
                elif choice == 'l':
                    await self.list_channels()
                
                elif choice == 't':
                    self.metrics.print_stats()
                
                elif choice == 'q':
                    print("👋 Saliendo...")
                    break
                
                else:
                    print("❌ Opción inválida")
                    
            except Exception as e:
                logging.error(f"Error en menú: {e}")
                print(f"❌ Error: {e}")
    
    async def run(self):
        """Ejecutar scraper"""
        self.display_ascii_art()
        
        # Configurar credenciales
        await self.setup_credentials()
        
        # Conectar cliente
        await self.client.start()
        print("✅ Cliente conectado")
        
        # Ejecutar menú principal
        await self.main_menu()
        
        # Cerrar cliente
        await self.client.disconnect()

# Función principal
async def main():
    config = ScraperConfig.from_file()
    scraper = TelegramScraper(config)
    await scraper.run()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Programa interrumpido")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Error fatal: {e}")
        print(f"❌ Error fatal: {e}")
        sys.exit(1)