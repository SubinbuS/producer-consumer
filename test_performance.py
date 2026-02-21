import os
import shutil
import multiprocessing as mp
import time
from producer_consumer import run_parallel_inversion


# ===========================
# Конфигурация
# ===========================

MAX_FILES = 10
MAX_PROCESSES = 8
TIMEOUT_SECONDS = 300


def find_existing_inputs():
    """
    Ищем input1.jpg ... input10.jpg
    """
    files = []
    for i in range(1, MAX_FILES + 1):
        filename = f"input{i}.jpg"
        if os.path.exists(filename):
            files.append(f"input{i}")
    return files


def run_single_test(test_number, num_workers, image_list):
    """
    Запускает один тест:
    - создаёт папку testN
    - запускает обработку
    - переносит output_* в testN
    """

    test_folder = f"test{test_number}"

    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)

    os.makedirs(test_folder)

    print(f"\n=== Тест {test_number} | Процессов: {num_workers} ===")

    start_time = time.perf_counter()

    process = mp.Process(
        target=run_parallel_inversion,
        args=(image_list, num_workers)
    )

    process.start()
    process.join(timeout=TIMEOUT_SECONDS)

    if process.is_alive():
        print("[ПРОБЛЕМА] Возможное зависание!")
        process.terminate()
        return

    end_time = time.perf_counter()
    total_time = end_time - start_time

    # переносим output файлы
    for file in os.listdir():
        if file.startswith("output_"):
            shutil.move(file, os.path.join(test_folder, file))

    print(f"Тест {test_number} завершён.")
    print(f"Общее время: {total_time:.3f} сек")
    print(f"Результаты сохранены в папку {test_folder}")


def main():
    mp.freeze_support()

    print("=== Автоматическое тестирование ===")

    images = find_existing_inputs()

    if not images:
        print("Файлы input1.jpg - input10.jpg не найдены.")
        return

    test_counter = 1

    # Тестируем от 1 до 8 процессов
    for workers in range(1, MAX_PROCESSES + 1):
        run_single_test(test_counter, workers, images)
        test_counter += 1

    print("\n=== Все тесты завершены ===")


if __name__ == "__main__":
    main()