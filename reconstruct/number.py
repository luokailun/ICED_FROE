
class ExtendedInfinity:
    def __init__(self, count=1, number=0):
        self.count = count
        self.number = number
    
    def __add__(self, other):
        if isinstance(other, ExtendedInfinity):
            return ExtendedInfinity(self.count + other.count, self.number + other.number)
        elif other == float('inf'):
            return ExtendedInfinity(self.count + 1, self.number)
        elif isinstance(other, (int, float)):
            return ExtendedInfinity(self.count, self.number+other)
        else:
            return NotImplemented
    
    def __sub__(self, other):
        if isinstance(other, ExtendedInfinity):
            count = self.count - other.count
            number = self.number - other.number
            if count ==0:
                return number
            else:
                return ExtendedInfinity(count, number)
        if isinstance(other, (int, float)):
            return ExtendedInfinity(self.count, self.number - other)
        else:
            return NotImplemented

    def __radd__(self, other):
        # 支持反向加法 int + ExtendedInfinity
        return self.__add__(other)

    def __rsub__(self, other):
        # 支持反向减法 int - ExtendedInfinity
        if isinstance(other, (int, float)):
            return ExtendedInfinity(-self.count, other - self.number)
        else:
            return NotImplemented
        
    def __repr__(self):
        if self.number != 0:
            return f"{self.count} * inf - {abs(self.number)}"
        return f"{self.count} * inf" if self.count > 1 else "inf"
    
    def __gt__(self, other):
        if isinstance(other, ExtendedInfinity):
            if self.count == other.count:
                return self.number > other.number
            return self.count > other.count
        elif isinstance(other, (int, float)):
            if self.count >0:
                return True  # 自定义的 inf 始终大于任何有限数
            else:
                return False
        else:
            return NotImplemented

    def __lt__(self, other):
        if isinstance(other, ExtendedInfinity):
            if self.count == other.count:
                return self.number < other.number
            return self.count < other.count
        elif isinstance(other, (int, float)):
            if self.count >0:
                return False  # 自定义的 inf 始终大于任何有限数
            else:
                return True
        else:
            return NotImplemented

    def __neg__(self):
        # 返回取负数的 ExtendedInfinity 实例
        return ExtendedInfinity(-self.count, -self.number)
    
    def __ge__(self, other):
        if isinstance(other, ExtendedInfinity):
            if self.count == other.count:
                return self.number >= other.number
            return self.count > other.count
        elif isinstance(other, (int, float)):
            return True  # 自定义的 inf 始终大于或等于任何有限数
        else:
            return NotImplemented

#测试

# infi = ExtendedInfinity()
# infi_minus_1 = infi - 1
# infi_minus_2 = infi - 2

# a = infi_minus_1 -infi_minus_2
# print(a)
# print(-infi_minus_1 < -infi_minus_2)
# exit(0)

# print(infi_minus_1)  # 输出: inf - 1
# print(infi_minus_2)  # 输出: inf - 2
# print(infi_minus_1 > infi_minus_2)  # 输出: True
