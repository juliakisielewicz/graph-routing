import geopy
import geopy.distance

start = geopy.Point(50.064651, 19.944981)
d = geopy.distance.distance(kilometers=90)

# bearing of 0 degrees is north
north = d.destination(point=start, bearing=0).format_decimal()
east = d.destination(point=start, bearing=90).format_decimal()
south = d.destination(point=start, bearing=180).format_decimal()
west = d.destination(point=start, bearing=270).format_decimal()

print(west)
print(south)
print(east)
print(north)
