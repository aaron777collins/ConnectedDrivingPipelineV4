from glob import glob
import importlib
import os
import sys
import traceback

from Helpers.BColors import BColors

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
                print(BColors.OKGREEN + f"Test {file} succeeded" + BColors.ENDC)
            else:
                print(BColors.FAIL + f"Test {file} failed with error:\n{result[1]}" + BColors.ENDC)
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
