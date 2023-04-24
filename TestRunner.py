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

        # print how many passed and how many failed
        print(BColors.OKGREEN + f"{len([result for result in results if result[1] == 'success'])} tests passed" + BColors.ENDC)

        failed = [result[0] for result in results if result[1] != 'success']
        if (len(failed) > 0):
            print(BColors.FAIL + f"{len(failed)} tests failed" + BColors.ENDC)
            print(BColors.FAIL + f"Failed tests: {failed}" + BColors.ENDC)

        return results

    def run_test(self, file: str) -> tuple[str, str]:
        # run test and return result with possible error

        class_name = file.split("/")[-1].split(".")[0]

        # import file
        module = importlib.import_module(f"Test.Tests.{class_name}")
        cls = getattr(module, class_name)

        # run the "run" method and catch errors and return result

        # sub function to run cleanup method and catch errors
        def cleanup():
            try:
                cls().cleanup()
            except Exception as e:
                error = traceback.format_exc()
                return (file, error)

        try:
            result = cls().run()
            cleanup()
            return (file, "success")
        except Exception as e:
            error = traceback.format_exc()
            cleanup()
            return (file, error)


if __name__ == "__main__":
    TestRunner().run_all_tests()
