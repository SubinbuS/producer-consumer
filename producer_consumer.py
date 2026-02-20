import threading
import queue
import time
import os
from dataclasses import dataclass
from typing import List
from PIL import Image


# -----------------------------
# Модель задачи
# -----------------------------
@dataclass
class Task:
    y_start: int
    y_end: int


# -----------------------------
# Алгоритм инверсии (Вариант 1)
# -----------------------------
def invert_pillow(image: Image.Image, y_start: int, y_end: int):
    pixels = image.load()
    width = image.width

    for y in range(y_start, y_end):
        for x in range(width):
            r, g, b = pixels[x, y]
            pixels[x, y] = (255 - r, 255 - g, 255 - b)


# -----------------------------
# Producer
# -----------------------------
class Producer(threading.Thread):
    def __init__(self, task_queue: queue.Queue,
                 image_height: int,
                 block_size: int):
        super().__init__()
        self.task_queue = task_queue
        self.image_height = image_height
        self.block_size = block_size

    def run(self):
        for y in range(0, self.image_height, self.block_size):
            y_end = min(y + self.block_size, self.image_height)
            task = Task(y_start=y, y_end=y_end)
            self.task_queue.put(task)


# -----------------------------
# Consumer
# -----------------------------
class Consumer(threading.Thread):
    def __init__(self, task_queue: queue.Queue,
                 image: Image.Image):
        super().__init__()
        self.task_queue = task_queue
        self.image = image

    def run(self):
        while True:
            task = self.task_queue.get()

            if task is None:
                self.task_queue.task_done()
                break

            invert_pillow(self.image, task.y_start, task.y_end)
            self.task_queue.task_done()


# -----------------------------
# Task Manager
# -----------------------------
class TaskManager:
    def __init__(self, image: Image.Image,
                 num_workers: int,
                 block_size: int,
                 queue_size: int = 100):

        if num_workers <= 0:
            raise ValueError("Number of workers must be >= 1")

        self.image = image
        self.task_queue = queue.Queue(maxsize=queue_size)
        self.num_workers = num_workers
        self.block_size = block_size

        self.producer = Producer(
            self.task_queue,
            image_height=image.height,
            block_size=block_size
        )

        self.consumers: List[Consumer] = [
            Consumer(self.task_queue, image)
            for _ in range(num_workers)
        ]

    def start(self):
        for consumer in self.consumers:
            consumer.start()

        self.producer.start()

    def wait_for_completion(self):
        self.producer.join()
        self.task_queue.join()

        # Poison pill
        for _ in self.consumers:
            self.task_queue.put(None)

        self.task_queue.join()

        for consumer in self.consumers:
            consumer.join()


# -----------------------------
# Универсальная функция для тестов
# -----------------------------
def process_image(input_path: str,
                  output_path: str,
                  num_workers: int,
                  block_size: int = 32) -> float:
    """
    Обрабатывает изображение и возвращает время обработки.
    Используется как для main, так и для тестов.
    """

    image = Image.open(input_path).convert("RGB")

    manager = TaskManager(
        image=image,
        num_workers=num_workers,
        block_size=block_size,
        queue_size=200
    )

    start_time = time.time()

    manager.start()
    manager.wait_for_completion()

    end_time = time.time()

    image.save(output_path)

    return end_time - start_time


# -----------------------------
# Main
# -----------------------------
def main():
    filename = input("Введите имя файла (без расширения): ").strip()
    input_path = filename + ".jpg"

    if not os.path.exists(input_path):
        print("Файл не найден.")
        return

    try:
        num_workers = int(input("Введите количество потоков: "))
    except ValueError:
        print("Некорректное число потоков.")
        return

    output_path = f"{filename}_inverted.jpg"

    duration = process_image(
        input_path=input_path,
        output_path=output_path,
        num_workers=num_workers
    )

    print(f"Использовано потоков: {num_workers}")
    print(f"Время обработки: {duration:.3f} секунд")
    print("Готово!")

if __name__ == "__main__":
    main()