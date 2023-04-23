from glob import glob
import importlib
import os
import sys
import traceback

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class TestRunner:

    # run all tests within the Test/Tests directory and keep track of the results and errors
    def run_all_tests(self):
        # get all files in the Test/Tests directory
        files = glob(pathname="*.py", root_dir="Test/Tests")

        print(f"Found {len(files)} tests: {files}")


        results:list[tuple[str, str]] = []

        for file in files:
            # run the test

            print(f"Running test: {file}")

            result = self.run_test(file)

            if(result[1] == "success"):
                print(bcolors.OKGREEN + f"Test {file} succeeded" + bcolors.ENDC)
            else:
                print(bcolors.FAIL + f"Test {file} failed with error:\n{result[1]}" + bcolors.ENDC)
            # add the result to the list
            results.append(result)

            return results

    def run_test(self, file: str) -> tuple[str, str]:
        # run test and return result with possible error

        class_name = file.split("/")[-1].split(".")[0]

        # import file
        module = importlib.import_module(f"Test.Tests.{class_name}")
        cls = getattr(module, class_name)

        # run the "run" method and catch errors and return result

        try:
            result = cls().run()
            return (file, "success")
        except Exception as e:
            error = traceback.format_exc()
            return (file, error)

if __name__ == "__main__":
    TestRunner().run_all_tests()
