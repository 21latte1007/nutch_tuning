input = './urls/output-2.txt'
output = './urls/output-2.txt'

with open(input, "r") as f:
    lines = f.readlines()
with open(output, "w") as f:
    for line in lines:
        m = line.replace("http://", "https://")
        f.write(m)
