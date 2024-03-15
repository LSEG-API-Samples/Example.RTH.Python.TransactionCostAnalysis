from abc import ABC, abstractmethod

class MetricBenchmarkBase(ABC):

    @property
    @abstractmethod
    def name(self):
        pass
 
    @abstractmethod
    def calculate(self, trades):
        pass