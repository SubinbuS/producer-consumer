import threading
import queue
import time
import random
from dataclasses import dataclass
from typing import List, Optional
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Task:
    """Класс для представления задачи"""
    id: int
    data: str
    created_at: float


class Producer(threading.Thread):
    """Производитель задач"""

    def __init__(self, task_queue: queue.Queue, producer_id: int,
                 tasks_to_produce: int = 10, delay: float = 0.5):
        super().__init__()
        self.task_queue = task_queue
        self.producer_id = producer_id
        self.tasks_to_produce = tasks_to_produce
        self.delay = delay
        self.daemon = True
        self.tasks_created = 0
        self.logger = logging.getLogger(f"Producer-{producer_id}")

    def run(self):
        """Запуск производителя"""
        self.logger.info(f"Запущен производитель {self.producer_id}")

        for i in range(self.tasks_to_produce):
            try:
                # Создание задачи
                task = Task(
                    id=i,
                    data=f"Data from producer {self.producer_id} - task {i}",
                    created_at=time.time()
                )

                # Помещение задачи в очередь с таймаутом
                self.task_queue.put(task, timeout=1.0)
                self.tasks_created += 1

                self.logger.debug(f"Создана задача {task.id}: {task.data}")

                # Имитация времени на создание задачи
                time.sleep(self.delay)

            except queue.Full:
                self.logger.error("Очередь переполнена, задача не добавлена")
            except Exception as e:
                self.logger.error(f"Ошибка при создании задачи: {e}")

        self.logger.info(f"Производитель {self.producer_id} завершил работу. "
                         f"Создано задач: {self.tasks_created}")


class Consumer(threading.Thread):
    """Потребитель задач"""

    def __init__(self, task_queue: queue.Queue, consumer_id: int,
                 processing_time: float = 1.0):
        super().__init__()
        self.task_queue = task_queue
        self.consumer_id = consumer_id
        self.processing_time = processing_time
        self.daemon = True
        self.tasks_processed = 0
        self.running = True
        self.logger = logging.getLogger(f"Consumer-{consumer_id}")

    def run(self):
        """Запуск потребителя"""
        self.logger.info(f"Запущен потребитель {self.consumer_id}")

        while self.running:
            try:
                # Получение задачи из очереди с таймаутом
                task = self.task_queue.get(timeout=0.5)

                # Обработка задачи
                self.process_task(task)

                # Отметка задачи как выполненной
                self.task_queue.task_done()
                self.tasks_processed += 1

            except queue.Empty:
                # Если очередь пуста, продолжаем ожидание
                continue
            except Exception as e:
                self.logger.error(f"Ошибка при обработке задачи: {e}")

        self.logger.info(f"Потребитель {self.consumer_id} завершил работу. "
                         f"Обработано задач: {self.tasks_processed}")

    def process_task(self, task: Task):
        """Обработка задачи"""
        processing_time = random.uniform(0.5, self.processing_time)
        time.sleep(processing_time)

    def stop(self):
        """Остановка потребителя"""
        self.running = False


class TaskManager:
    """Менеджер для управления производителями и потребителями"""

    def __init__(self, max_queue_size: int = 100):
        self.task_queue = queue.Queue(maxsize=max_queue_size)
        self.producers: List[Producer] = []
        self.consumers: List[Consumer] = []
        self.logger = logging.getLogger("TaskManager")

    def add_producer(self, producer_id: int, tasks_to_produce: int = 10,
                     delay: float = 0.5) -> Producer:
        """Добавление производителя"""
        producer = Producer(self.task_queue, producer_id,
                            tasks_to_produce, delay)
        self.producers.append(producer)
        return producer

    def add_consumer(self, consumer_id: int, processing_time: float = 1.0) -> Consumer:
        """Добавление потребителя"""
        consumer = Consumer(self.task_queue, consumer_id, processing_time)
        self.consumers.append(consumer)
        return consumer

    def start_all(self):
        """Запуск всех производителей и потребителей"""
        self.logger.info("Запуск всех потоков...")

        # Запуск потребителей
        for consumer in self.consumers:
            consumer.start()

        # Запуск производителей
        for producer in self.producers:
            producer.start()

    def wait_for_completion(self, timeout: Optional[float] = None):
        """Ожидание завершения всех задач"""
        self.logger.info("Ожидание завершения всех задач...")

        # Ожидание завершения всех производителей
        for producer in self.producers:
            producer.join(timeout=timeout)

        # Ожидание обработки всех задач в очереди
        self.task_queue.join()

        # Остановка потребителей
        for consumer in self.consumers:
            consumer.stop()

        # Ожидание завершения потребителей
        for consumer in self.consumers:
            consumer.join(timeout=timeout)

        self.logger.info("Все задачи выполнены")

    def get_statistics(self) -> dict:
        """Получение статистики работы"""
        total_produced = sum(p.tasks_created for p in self.producers)
        total_consumed = sum(c.tasks_processed for c in self.consumers)

        return {
            'queue_size': self.task_queue.qsize(),
            'total_producers': len(self.producers),
            'total_consumers': len(self.consumers),
            'tasks_produced': total_produced,
            'tasks_consumed': total_consumed,
            'tasks_in_queue': total_produced - total_consumed
        }


def main():
    """Основная функция для демонстрации работы"""

    # Создание менеджера задач
    manager = TaskManager(max_queue_size=20)

    # Добавление производителей
    num_producers = 3
    for i in range(num_producers):
        # Разное количество задач для каждого производителя
        tasks_count = random.randint(5, 15)
        manager.add_producer(
            producer_id=i,
            tasks_to_produce=tasks_count,
            delay=random.uniform(0.2, 0.8)
        )

    # Добавление потребителей
    num_consumers = 4
    for i in range(num_consumers):
        manager.add_consumer(
            consumer_id=i,
            processing_time=random.uniform(0.5, 2.0)
        )

    # Запуск всех потоков
    manager.start_all()

    # Мониторинг процесса
    try:
        while any(p.is_alive() for p in manager.producers):
            stats = manager.get_statistics()
            logger.info(f"Статистика: {stats}")
            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Программа прервана пользователем")

    # Ожидание завершения
    manager.wait_for_completion(timeout=5.0)

    # Финальная статистика
    final_stats = manager.get_statistics()
    logger.info(f"Финальная статистика: {final_stats}")


if __name__ == "__main__":
    main()