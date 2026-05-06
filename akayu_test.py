import akayu

s = akayu.Stream()
result = s.map(lambda x: x ** 2).filter(lambda x: x > 10).to_list()

with s:
    s.emit_batch([1, 2, 3, 4, 5])

print("Results:", list(result))