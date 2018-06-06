import ast
import sys

argv = sys.argv[1:]
base = argv[0] + "/part-r-0000"
myset = set()
maior = (0,0)
maior_life = (0,0)
maior_love = (0,0)

for index in range(int(argv[1])):
    with open(base + str(index), 'r') as f:
        print base + str(index)
        for line in f:
            reg = line.strip().split("	")

            if len(reg) > 1:
                words = reg[0].strip().split(',')
                value = float(reg[1].strip())

                if words[0][1:].strip() == "life" and value > maior_life[1]:
                    maior_life = (words[1][:-2].strip(), value)
                elif words[1][:-2].strip() == "life" and value > maior_life[1]:
                    maior_life = (words[0][1:].strip(), value)

                if words[0][1:].strip() == "love" and value > maior_love[1]:
                    maior_love = (words[1][:-2].strip(), value)
                elif words[1][:-2].strip() == "love" and value > maior_love[1]:
                    maior_love = (words[0][1:].strip(), value)

                myset.add(reg[0])

                if value > maior[1]:
                    maior = (reg[0], value)

print "Total", len(myset)
print "Maior PMI", maior
print "Maior Life", maior_life
print "Maior Love", maior_love
