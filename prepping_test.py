def test_cast_doubles():
    coords = {'coordinates': [1,2,3], 'type': 'foo'}
    assert cast_doubles(coords) == coords
    assert type(cast_doubles(coords)['coordinates'][0]) == float

    coords = {'coordinates': [[1,2], [3, 4]], 'type': 'foo'}
    assert cast_doubles(coords)  == coords

def test_cast_coords():
    # TODO
    assert False
