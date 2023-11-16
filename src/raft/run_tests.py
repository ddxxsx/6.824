import sys
import threading
import os
import multiprocessing


def run_tests(Debug,Test,Start,End):
    for i in range(Start,End):
        if Debug:
            cmd_str = f"VERBOSE=1 go test -run {Test}>output_{Test}{i}.log"
        else:
            cmd_str = f"go test -run {Test}>output_{Test}{i}.log"
        os.system(cmd_str)
        # p = subprocess.Popen(cmd_str,
        #                      shell=True,
        #                      stdout=subprocess.PIPE,
        #                      stderr=subprocess.STDOUT
        #                      )
def divide_into_intervals(number, parts=5):
    interval = number // parts
    intervals = []

    for i in range(parts):
        start = i * interval
        if i == parts - 1:
            end = number
        else:
            end = (i + 1) * interval
        intervals.append((start, end))

    return intervals


def main():
    Debug = False
    if  len(sys.argv)<4:
        exit()
    if (sys.argv[1]=='Debug'):
        Debug = True
    Test = sys.argv[2]
    number = int(sys.argv[3])
    intervals = divide_into_intervals(number)
    print(intervals)
    threads = []
    length = len(intervals)
    for i in range(length):
        td1 = multiprocessing.Process(target=run_tests, args=(Debug,Test,intervals[i][0],intervals[i][1]))
        threads.append(td1)
        td1.start()
#        td1.join()

#    td1 = threading.Thread(target=run_tests, args=(Debug))
#    td1.start()
#    td1.join()
    for td in threads:
        td.join()

    print("main exit")


if __name__ == '__main__':
    main()
