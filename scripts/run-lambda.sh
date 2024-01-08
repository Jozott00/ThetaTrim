if [ -z "$1" ]; then
    echo "Usage: $0 <FunctionName>"
    exit 1
fi

FunctionName="$1"

if [ ! -f "event.json" ]; then
    touch event.json
    echo "{}" > event.json
    echo "event.json not found. Created a new file with empty JSON."
fi

cdk synth
cd cdk.out
sam local invoke "$FunctionName" --event ../event.json -t ThetaTrimStack.template.json
cd ..