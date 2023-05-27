import sys

def writeFile():
    print("Expected arguments: <fileName> <days> <hours> <minutes> <cpus> <memory(G)> [dependency]")
    if len(sys.argv) < 7:
        print("Not enough arguments. Please specify the file you want to make.")
        return

    fileName = sys.argv[1]
    days = sys.argv[2]
    hours = sys.argv[3]
    minutes = sys.argv[4]
    cpus = sys.argv[5]
    memory = sys.argv[6]
    dependency = ""
    if len(sys.argv) == 8:
        dependency = sys.argv[7]

    print(f"fileName: {fileName}")
    print(f"days: {days}")
    print(f"hours: {hours}")
    print(f"minutes: {minutes}")
    print(f"cpus: {cpus}")
    print(f"memory: {memory}")

    print(f"dependency: {dependency}")



    # Creating the file
    fileContents = f"#!/bin/bash\n\
#SBATCH -J MachineLearningPipelineV4\n\
#SBATCH --account=def-arunita\n\
#SBATCH --cpus-per-task={cpus}\n\
#SBATCH --mem={memory}G # 512GiB of memory\n\
#SBATCH -t {days}-{hours.zfill(2)}:{minutes.zfill(2)} # Running time of 10 min is 0-00:10\n"
    if dependency != "":
        fileContents = fileContents + f"#SBATCH --dependency={dependency}\n"
    fileContents = fileContents + f"module load python/3.10\n\
source ENV/bin/activate\n\
pip install -r requirements.txt\n\
python {fileName}.py"

    with open(f"slurmstart-{fileName}.sh", "w") as file:
        file.write(fileContents)

    print("File created! Contents:")
    print(fileContents)

if __name__ == "__main__":
    writeFile()
