class Print:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'

    @staticmethod
    def success(message):
        print(f"{Print.GREEN}[SUCCESS] {message}{Print.RESET}")

    @staticmethod
    def error(message):
        print(f"{Print.RED}[ERROR] {message}{Print.RESET}")

    @staticmethod
    def warning(message):
        print(f"{Print.YELLOW}[WARNING] {message}{Print.RESET}")