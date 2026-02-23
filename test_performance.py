import os
import shutil
import multiprocessing as mp
import time
from producer_consumer import run_parallel_inversion
import random

MAX_FILES = 10
MAX_PROCESSES = 8
TIMEOUT_SECONDS = 300

SUPPORTED_EXTENSIONS = ['.jpg', '.jpeg', '.png']


def find_all_inputs():
    files = []

    for i in range(1, MAX_FILES + 1):
        found = False
        for ext in SUPPORTED_EXTENSIONS:
            filename = f"input{i}{ext}"
            if os.path.exists(filename):
                files.append(f"input{i}")
                found = True
                break

        if not found:
            print(f"Предупреждение: файл input{i} не найден (ни .jpg, ни .png)")
    return files


def find_all_inputs_with_extensions():
    files = []
    for i in range(1, MAX_FILES + 1):
        for ext in SUPPORTED_EXTENSIONS:
            filename = f"input{i}{ext}"
            if os.path.exists(filename):
                files.append(f"input{i}{ext}")
                break
    return files


def select_random_subset(all_files, min_count=3, max_count=None):
    if max_count is None:
        max_count = len(all_files)

    count = random.randint(min_count, max_count)
    return random.sample(all_files, min(count, len(all_files)))


def get_files_for_test(test_number, all_images):
    if test_number == 1:
        return select_random_subset(all_images, min_count=2, max_count=4)
    elif test_number == 2:
        return select_random_subset(all_images, min_count=3, max_count=5)
    elif test_number == 3:
        return select_random_subset(all_images, min_count=4, max_count=6)
    elif test_number == 4:
        return select_random_subset(all_images, min_count=5, max_count=8)

    elif test_number == 5:
        return all_images[:3]
    elif test_number == 6:
        return all_images[:5]
    elif test_number == 7:
        return all_images[:7]
    elif test_number == 8:
        return all_images[:min(10, len(all_images))]
    else:
        return all_images[:test_number]


def check_files_exist(image_list):
    missing = []
    for img in image_list:
        found = False
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                found = True
                break
        if not found:
            missing.append(img)

    if missing:
        print(f"ВНИМАНИЕ: Следующие файлы не найдены: {missing}")
        return False
    return True


def run_single_test(test_number, num_workers, all_images):
    test_folder = f"test{test_number}"

    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)

    os.makedirs(test_folder)

    image_list = get_files_for_test(test_number, all_images)

    if not check_files_exist(image_list):
        print(f"ПРОПУСК теста {test_number}: отсутствуют файлы")
        return

    test_type = "СЛУЧАЙНЫЙ" if test_number <= 4 else "УВЕЛИЧЕНИЕ"

    print(f"\n{'=' * 60}")
    print(f"Тест {test_number} | Процессов: {num_workers} | Тип: {test_type}")
    print(f"{'=' * 60}")
    print(f"Обрабатывается файлов: {len(image_list)}")

    files_with_ext = []
    for img in image_list:
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                files_with_ext.append(f"{img}{ext}")
                break

    print(f"Файлы: {files_with_ext}")
    print(f"Количество процессов: {num_workers}")

    start_time = time.perf_counter()

    process = mp.Process(
        target=run_parallel_inversion,
        args=(image_list, num_workers)
    )

    process.start()
    process.join(timeout=TIMEOUT_SECONDS)

    if process.is_alive():
        print(f"[ПРОБЛЕМА] Возможное зависание! Процесс не завершился за {TIMEOUT_SECONDS} сек.")
        process.terminate()
        process.join()
        return

    end_time = time.perf_counter()
    total_time = end_time - start_time

    moved_files = 0
    for file in os.listdir():
        if file.startswith("output_"):
            if os.path.isfile(file):
                shutil.move(file, os.path.join(test_folder, file))
                moved_files += 1

    print(f"\nРезультаты теста {test_number}:")
    print(f"  - Время выполнения: {total_time:.3f} сек")
    print(f"  - Обработано файлов: {len(image_list)}")
    print(f"  - Получено результатов: {moved_files}")
    print(f"  - Результаты сохранены в папку: {test_folder}")
    if moved_files > 0:
        print(f"  - Среднее время на файл: {total_time / moved_files:.3f} сек")


def main():
    mp.freeze_support()

    print("=== Автоматическое тестирование (поддержка JPG и PNG) ===")
    print(f"Поддерживаемые форматы: {SUPPORTED_EXTENSIONS}")
    print("Тесты 1-4: случайные наборы файлов (разного размера)")
    print("Тесты 5-8: увеличивающееся количество файлов (3, 5, 7, все)")
    print()

    all_images = find_all_inputs()

    if not all_images:
        print("Файлы input1.jpg/input1.png - input10.jpg/input10.png не найдены.")
        print("Убедитесь, что у вас есть файлы с именами input1.jpg, input2.png и т.д.")
        return

    print(f"Найденные файлы (без расширения): {all_images}")
    print(f"Всего файлов: {len(all_images)}")

    print("\nФайлы с расширениями:")
    for img in all_images:
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                print(f"  - {img}{ext}")
                break

    random.seed(42)
    test_counter = 1

    for workers in range(1, MAX_PROCESSES + 1):
        run_single_test(test_counter, workers, all_images)
        test_counter += 1

    print("\n=== Все тесты завершены ===")


if __name__ == "__main__":
    main()