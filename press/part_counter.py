import functools


def locate(location):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 注入 location
            kwargs["location"] = location
            return func(*args, **kwargs)
        return wrapper
    return decorator


PART_COUNTER_BIAS = {
        "conveyer": 0,
        "shuttle": 1,
    }

class PartCounter:
    @classmethod
    def get_counter(cls, counter: int, location: str) -> int:
        location = location.lower()

        if location.lower() not in PART_COUNTER_BIAS:
            raise ValueError(f"location[{location}] is illegal. only {list(PART_COUNTER_BIAS.keys())} is defined.")

        bias = PART_COUNTER_BIAS[location]
        return counter + bias

    @classmethod
    @locate(location="shuttle")
    def on_shuttle(cls, counter: int, **kwargs) -> int:
        return cls.get_counter(counter=counter, **kwargs)


if __name__ == "__main__":
    print(PartCounter.on_shuttle(1))
