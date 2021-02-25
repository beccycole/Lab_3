from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occupancy_BHMBCCMKT01,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_occupancy_BHMBCCMKT01(self, _, line):
        
        data = DATA_RE.findall(line)
        if "BHMBCCMKT01" in data:
            occ = float(data[2])
            yield ("occupancy", occ)

    def reducer_get_avg(self, key, values):
        
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("Average occupancy of carpark", round(total,1) / size)
if __name__ == '__main__':
    MRProb2_3.run()