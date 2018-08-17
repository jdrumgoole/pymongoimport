import argparse

def make_line_file(count=1, doseol=False, filename="liner.txt"):
    with open( filename, "w") as file :
        for i in range(count):
            if doseol:
                file.write( "{}\r\n".format(i+1))
            else:
                file.write("{}\n".format(i + 1))

    return filename

if __name__ == "__main__" :

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", default=1, type=int)
    parser.add_argument("--filename", default="liner.txt")
    parser.add_argument("--doseol", default=False, action="store_true")
    args=parser.parse_args()

    make_line_file(args.count, args.doseol, args.filename)
    print("Created '{}' with {} line(s) and DOS EOL: '{}'".format( args.filename, args.count, args.doseol))



