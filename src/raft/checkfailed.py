import os

def main():
    current_directory = os.getcwd()
    for root, dirs, files in os.walk(current_directory):
        for file in files:
            if file.endswith(".log"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    content = f.read()
                    if "FAIL" in content:
                        print("filename: ",file_path)

if __name__ == "__main__":
    main()