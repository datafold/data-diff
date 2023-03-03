"""Contains the implementation of two classes:

- LexicographicSpace - a lexicographic space of arbitrary dimensions.
- BoundedLexicographicSpace - a lexicographic space, where the lowest point may be non-zero.

A lexicographic space is a space of increasing natural values, ordered by lexicographic order.
Read more: https://mathworld.wolfram.com/LexicographicOrder.html

These abstractions were written to support compound keys in the hashdiff algorithm.
In the hashdiff algorithm, we rely on the order of the column keys, to segment the table correctly.
SQL orders the columns of tables based on lexicographic ordering.
Since we need an evenly spaced "range" function over the space, which has arbitrary dimensions, we have
to implement it ourself.

As a further optimization, since we each time operate on segments of the ordered table, we add support
for working with a restricted space, which will reduce the likelihood of gaps in our "select", when the
keys are not evenly distributed.
"""

from random import randint, randrange

from typing import Tuple
from .utils import safezip

Vector = Tuple[int]
Interval = Tuple[int]


class Overflow(ValueError):
    pass


def neg_interval(interval):
    return tuple(-i for i in interval)


def neg_v(v: Vector):
    return tuple(-i for i in v)


def sub_v(v1: Vector, v2: Vector):
    return tuple(i1 - i2 for i1, i2 in safezip(v1, v2))


def add_v(v1: Vector, v2: Vector):
    return tuple(i1 + i2 for i1, i2 in safezip(v1, v2))


def rand_v_in_range(v1: Vector, v2: Vector):
    return tuple(irandrange(i1, i2) for i1, i2 in safezip(v1, v2))


def irandrange(start, stop):
    if start == stop:
        return start
    return randrange(start, stop)


class LexicographicSpace:
    """Lexicographic space of arbitrary dimensions.

    All elements must be of the same length as the number of dimensions. (no rpadding)
    """

    def __init__(self, dims: Vector):
        self.dims = dims

    def __contains__(self, v: Vector):
        return all(0 <= i < d for i, d in safezip(v, self.dims))

    def add(self, v1: Vector, v2: Vector) -> Vector:
        # assert v1 in self and v2 in self, (v1, v2)

        carry = 0
        res = []
        for i1, i2, d in reversed(list(safezip(v1, v2, self.dims))):
            n = i1 + i2 + carry
            carry = n // d
            assert carry <= 1
            n %= d
            res.append(n)

        if carry:
            raise Overflow("Overflow")

        new_v = tuple(reversed(res))
        assert new_v in self
        return new_v

    def sub(self, v1: Vector, v2: Vector):
        return self.add(v1, neg_v(v2))

    def _divide(self, v: Vector, count: int):
        n = 0
        for x, d in zip(v, self.dims[1:] + (1,), strict=True):
            x += n
            rem = x % count
            n = rem * d
            yield x // count

    def divide(self, v: Vector, count: int) -> Vector:
        return tuple(self._divide(v, count))

    def range(self, min_value: Vector, max_value: Vector, count: int):
        assert min_value in self and max_value in self
        count -= 1
        size = self.sub(max_value, min_value)
        interval = self.divide(size, count)
        n = min_value
        for i in range(count):
            yield n
            n = self.add(n, interval)
        yield n


class BoundedLexicographicSpace:
    """Lexicographic space of arbitrary dimensions, where the lowest point may be non-zero.

    i.e. a space resticted by a "bounding-box" between two arbitrary points.
    """

    def __init__(self, min_bound: Vector, max_bound: Vector):
        dims = tuple(mx - mn for mn, mx in safezip(min_bound, max_bound))
        if not all(d >= 0 for d in dims):
            raise ValueError("Error: Negative dimension!")
        if not (dims[0] > 0):
            raise ValueError("First dimension must be non-zero!")

        self.min_bound = min_bound
        self.max_bound = max_bound

        self.uspace = LexicographicSpace(dims)

    def __contains__(self, p: Vector):
        return all(mn <= i < mx for i, mn, mx in safezip(p, self.min_bound, self.max_bound))

    def to_uspace(self, v: Vector) -> Vector:
        assert v in self
        return sub_v(v, self.min_bound)

    def from_uspace(self, v: Vector) -> Vector:
        res = add_v(v, self.min_bound)
        assert res in self
        return res

    def add_interval(self, v1: Vector, interval: Interval) -> Vector:
        return self.from_uspace(self.uspace.add(self.to_uspace(v1), interval))

    def sub_interval(self, v1: Vector, interval: Interval) -> Vector:
        return self.from_uspace(self.uspace.sub(self.to_uspace(v1), interval))

    def sub(self, v1: Vector, v2: Vector) -> Interval:
        return self.uspace.sub(self.to_uspace(v1), self.to_uspace(v2))

    def range(self, min_value: Vector, max_value: Vector, count: int):
        return [
            self.from_uspace(v) for v in self.uspace.range(self.to_uspace(min_value), self.to_uspace(max_value), count)
        ]


def test_lex_space():
    # Test add
    binspace = LexicographicSpace((2, 2, 2, 2))
    zero = (0, 0, 0, 0)
    one = (0, 0, 0, 1)
    bin_nums = [zero]
    for i in range(15):
        last = bin_nums[-1]
        bin_nums.append(binspace.add(last, one))
    five = bin_nums[5]
    seven = bin_nums[7]
    eight = bin_nums[8]
    fifteen = bin_nums[15]

    assert binspace.add(binspace.add(one, five), one) == seven
    assert binspace.add(one, seven) == eight
    assert binspace.add(seven, eight) == fifteen

    assert binspace.sub(eight, one) == seven
    assert binspace.sub(fifteen, seven) == eight

    r = list(binspace.range(one, seven, 4))
    assert r == [one, bin_nums[3], five, seven], r

    decspace = LexicographicSpace((10, 10, 10))
    assert decspace.divide((4, 5, 2), 2) == (2, 2, 6)
    assert decspace.divide((3, 0, 2), 2) == (1, 5, 1)

    # Restricted space

    rspace1 = BoundedLexicographicSpace((2, 2), (8, 8))
    assert rspace1.add_interval((2, 2), (0, 0)) == (2, 2)
    assert rspace1.add_interval((2, 2), (0, 1)) == (2, 3)
    assert rspace1.add_interval((2, 2), (0, 6)) == (3, 2)
    assert rspace1.add_interval((2, 2), (0, 7)) == (3, 3)
    # space.add((2,2), (6, 0))    # Overflow

    rspace2 = BoundedLexicographicSpace((4, 4, 4, 4), (6, 6, 6, 6))
    _one = (4, 4, 4, 5)
    _three = (4, 4, 5, 5)
    _five = (4, 5, 4, 5)
    _seven = (4, 5, 5, 5)
    assert rspace2.add_interval(rspace2.add_interval(_five, one), one) == _seven
    assert rspace2.sub_interval(rspace2.sub_interval(_seven, one), one) == _five

    r = list(rspace2.range(_one, _seven, 4))
    assert r == [_one, _three, _five, _seven], r

    # Test range -
    # For random bounds and min/max values, assert that range() generates steps with uniform distances
    MAX_COLUMNS = 16
    MAX_DIM = 10000
    MAX_BISECTION = 128

    for n in range(1, MAX_COLUMNS):
        min_bound = tuple(randint(0, MAX_DIM) for i in range(n))
        size = tuple(randint(1, MAX_DIM) for i in range(n))
        max_bound = add_v(min_bound, size)

        sp = BoundedLexicographicSpace(min_bound, max_bound)

        max_value = rand_v_in_range(min_bound, max_bound)
        min_value = rand_v_in_range(min_bound, max_value)
        for count in range(2, MAX_BISECTION):
            r = sp.range(min_value, max_value, count)
            assert len(r) == count
            diffs = [sp.sub(b, a) for a, b in zip(r[:-1], r[1:])]
            assert len(set(diffs)) == 1  # Uniform!
            # print('.', end='')


if __name__ == "__main__":
    test_lex_space()
