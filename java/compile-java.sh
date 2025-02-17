# compile all java files in supplied directory and subdirectories into bin/ directory and run the program
# Usage: ./compile.sh <directory>
if [ $# -ne 1 ]; then
		echo "Usage: ./compile.sh <directory>"
		exit 1
fi

java_files=$(find $1 -name "*.java")

if [ -z "$java_files" ]; then
		echo "No java files found in $1"
		exit 1
fi

mkdir -p bin

javac -d bin $java_files

if [ $? -ne 0 ]; then
		echo "Compilation failed"
		exit 1
fi


