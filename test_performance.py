import time
import os
import threading
from producer_consumer import process_image

# ===========================
# Настройки тестов
# ===========================

TEST_CONFIG = {
    "input1.jpg": [1, 4, 8],
    "input2.jpg": [2, 6],
    "input3.jpg": [1, 2, 4, 8]
}

TIMEOUT_SECONDS = 60


def run_test(image_path, num_threads):
    print(f"\n--- {image_path} | Потоки: {num_threads} ---")

    if not os.path.exists(image_path):
        print(f"[ОШИБКА] Файл {image_path} не найден")
        return

    result = {}

    def target():
        duration = process_image(
            input_path=image_path,
            output_path=f"test_output_{num_threads}.jpg",
            num_workers=num_threads
        )
        result["duration"] = duration

    test_thread = threading.Thread(target=target)
    test_thread.start()
    test_thread.join(timeout=TIMEOUT_SECONDS)

    if test_thread.is_alive():
        print("[ПРОБЛЕМА] Возможное зависание потока!")
        return

    print(f"[OK] Время обработки: {result['duration']:.3f} секунд")


def main():
    print("=== Тестирование производительности ===")

    for image, thread_list in TEST_CONFIG.items():
        for threads in thread_list:
            run_test(image, threads)

    print("\n=== Тестирование завершено ===")


if __name__ == "__main__":
    main()