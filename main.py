import subprocess

def run_script(script_name):
    try:
        result = subprocess.run(["python", script_name], check=True, text=True, capture_output=True)
        print(f"Output of {script_name}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_name}: {e.stderr}")
    except FileNotFoundError:
        print(f"Script {script_name} not found!")

if __name__ == "__main__":
    run_script("extraction\ehr_extract.py")
    # run_script("b.py")
