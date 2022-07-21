import copy
import csv
import os
import shutil
from os.path import exists
from nbt import nbt
import zlib
import io
import math
import time

from nbt.nbt import *


def copy_nbt_file(object):
    object.write_file('temp_object')
    return nbt.NBTFile('temp_object')
    os.remove('temp_object')


def copy_region(chunk_list):
    new_region = []
    for i in range(0, 1024):
        new_region.append(copy_nbt_file(chunk_list[i]))
    return new_region


def convert_to_region_coords(x, z):
    x, z = convert_to_chunk_coords(x, z)
    return x >> 5, z >> 5


def convert_to_chunk_coords(x, z):
    chunk_x = math.floor(x / 16)
    chunk_z = math.floor(z / 16)

    return chunk_x, chunk_z


def region_chunknum_to_chunk_coords(region_x, region_z, chunknum):
    x_in_region = chunknum % 32
    z_in_region = (chunknum/32).__floor__()
    x_offset, z_offset = region_x*32, region_z*32
    return x_offset+x_in_region, z_offset+z_in_region


def get_section(y):
    section = math.floor(y / 16) + 4

    return section


def get_chunk_number(world_x, world_z):
    chunk_x, chunk_z = convert_to_chunk_coords(world_x, world_z)

    chunk_number = (chunk_z * 32) + chunk_x

    return chunk_number


def get_xyz_in_subchunk(x, y, z):
    return x % 16, y % 16, z % 16


def global_xyz_to_chunk_offset(world_x, world_y, world_z):
    x, y, z = get_xyz_in_subchunk(world_x, world_y, world_z)

    block_number_in_chunk = (y * 256) + (z * 16) + x
    return block_number_in_chunk


def find_int_num(x, y, z, bits):
    block_number_in_chunk = global_xyz_to_chunk_offset(x, y, z)
    blocks_per_int = math.floor(64 / bits)

    int_where_block_is_found = (block_number_in_chunk / blocks_per_int).__floor__()

    return int_where_block_is_found


def find_position_in_int(x, y, z, bits):
    x, y, z = get_xyz_in_subchunk(x, y, z)

    block_number_in_chunk = (y * 256) + (z * 16) + x
    # floor is needed as masking requires an integer (and other reasons which I realized later)
    blocks_per_int = (64 / bits).__floor__()

    block_within_int = block_number_in_chunk % blocks_per_int

    return block_within_int


def get_nbt_fromfile(path):
    compressed_file = open(path, 'rb')
    decompressed_file = zlib.decompress(compressed_file.read())
    data_buffer = io.BytesIO(decompressed_file)
    nbtfile = nbt.NBTFile(buffer=data_buffer)
    compressed_file.close()
    return nbtfile


def get_nbt(data):
    decompressed_file = zlib.decompress(data)
    data_buffer = io.BytesIO(decompressed_file)
    nbtfile = nbt.NBTFile(buffer=data_buffer)
    return nbtfile


def generate_nbt_data(nbtobject):
    data_buffer = io.BytesIO()
    if nbtobject != 0:
        nbtobject.write_file(buffer=data_buffer)
        compressed_object = zlib.compress(data_buffer.getvalue())
    else:
        compressed_object = 0
    return compressed_object


def output_nbt_file(nbtobject, path):
    compressed_object = generate_nbt_data(nbtobject)

    outfile = open(path, 'wb')
    outfile.write(compressed_object)
    #print(len(compressed_object))
    outfile.close()


def open_mca(path):
    file = open(path, 'rb')
    chunk_locations = []
    chunk_sizes = []
    for i in range(0, 1024):
        chunk_locations.append(int.from_bytes(file.read(3), 'big') * 4096)
        chunk_sizes.append(int.from_bytes(file.read(1), 'big'))

    file.seek(0)
    region_bytes = file.read()

    chunks = []

    for i in range(0, 1024):
        if chunk_sizes[i] != 0:
            chunk_length = int.from_bytes(region_bytes[chunk_locations[i]:chunk_locations[i] + 4], 'big') - 1
            nbtdata = get_nbt(region_bytes[chunk_locations[i] + 5:chunk_locations[i] + chunk_length + 5])
            x = nbtdata['xPos'].value
            z = nbtdata['zPos'].value
            chunks.append(nbtdata)
            # print(x, z, hex(chunk_locations[i]))
        else:
            chunks.append(0)
    file.close()
    return chunks


def generate_mca(chunk_list, path):
    # create file in binary mode, fails if file already exists
    newfile = open(path, 'xb')
    pos = 2
    chunk_datas = []
    chunk_memory_sizes = []
    for i in range(0, 1024):
        chunk_datas.append(generate_nbt_data(chunk_list[i]))
        if chunk_datas[i] != 0:
            bytes_required = len(chunk_datas[i]) + 5
        else:
            bytes_required = 1000
        chunk_memory_sizes.append(math.ceil(bytes_required / 4096))
        length = chunk_memory_sizes[i]
        newfile.write(pos.to_bytes(3, 'big'))
        newfile.write(length.to_bytes(1, 'big'))
        pos += length
    current_time = math.floor(time.time()).to_bytes(4, 'big')
    for i in range(0, 1024):
        newfile.write(current_time)
    for i in range(0, 1024):
        if chunk_datas[i] != 0:
            chunk_size = len(chunk_datas[i]) + 1
        else:
            chunk_size = 0
        compression_type = b'\x02'
        newfile.write(chunk_size.to_bytes(4, 'big'))
        newfile.write(compression_type)
        if chunk_datas[i] != 0:
            newfile.write(chunk_datas[i])
            zeroes = (chunk_memory_sizes[i] * 4096) - (chunk_size + 4)
            newfile.write((0).to_bytes(zeroes, 'big'))
        else:
            newfile.write((0).to_bytes(4091, 'big'))
    newfile.close()


def turn_everything_to_air(chunk_list):
    for chunk in chunk_list:
        for section in chunk['sections']:
            for block in section['block_states']['palette']:
                block['Name'].value = 'minecraft:air'


def print_blocks(chunk_list, number, section):
    data = chunk_list[number]['sections'][section]['block_states']['data']
    print(data)
    for i in range(0, 16):
        print(extract_block(data[255], i, 4))


def turn_chunk_to_stone(chunk_list, number, section):
    chunk_list[number]['sections'][section]['block_states']['palette'][0]['Name'].value = 'minecraft:stone'


def extract_block(data_list, bits, block_number_in_chunk):
    blocks_per_int = (64 / bits).__floor__()
    int_where_block_is_found = (block_number_in_chunk / blocks_per_int).__floor__()
    block_within_int = block_number_in_chunk % blocks_per_int
    digit = block_within_int * bits
    data = data_list[int_where_block_is_found]
    block_id = (data & (((1 << bits) - 1) << digit)) >> digit
    return block_id


def replace_block_id(data, pos, bits, new_id):
    mask = (2 ** 64 - 1) - (2 ** (bits * (pos + 1)) - 1) + (2 ** (bits * pos) - 1)
    data = data & mask
    data = data | (new_id << (bits*pos))
    return data


def replace_block_indata(data_list, bits, block_number_in_chunk, new_id):
    blocks_per_int = (64 / bits).__floor__()
    int_where_block_is_found = (block_number_in_chunk / blocks_per_int).__floor__()
    data = data_list[int_where_block_is_found]
    block_within_int = block_number_in_chunk % blocks_per_int
    mask = (2 ** 64 - 1) - (2 ** (bits * (block_within_int + 1)) - 1) + (2 ** (bits * block_within_int) - 1)
    data = data & mask
    data = data | (new_id << (bits * block_within_int))

    data = int.from_bytes(data.to_bytes(8, 'big', signed=False), 'big', signed=True)
    data_list[int_where_block_is_found] = data


def print_block_value(x, y, z, world_folder):
    region_x, region_z = convert_to_region_coords(x, z)

    chunk_list = open_mca(f'{world_folder}/region/r.{region_x}.{region_z}.mca')

    chunk_num = get_chunk_number(x, z)
    section = get_section(y)
    block_data = chunk_list[chunk_num]['sections'][section]['block_states']['data']
    palette = chunk_list[chunk_num]['sections'][section]['block_states']['palette']

    block_int_pos = find_int_num(x, y, z, 4)
    block_int = block_data[block_int_pos]

    pos_in_int = find_position_in_int(x, y, z, 4)

    block_id = extract_block(block_int, pos_in_int, 4)

    print(palette[block_id]['Name'].value)


def new_block_states(block_id):
    state = TAG_Compound(name='block_states')

    data = TAG_Long_Array(name='data')
    data.value = [0] * 256

    palette = TAG_List(name="palette", type=TAG_Compound)
    palette.tags.extend([TAG_Compound(), TAG_Compound()])
    palette[0].tags.extend([TAG_String(name='Name', value='minecraft:air')])
    palette[1].tags.extend([TAG_String(name='Name', value=block_id)])

    state.tags.extend([data, palette])

    return state


def set_block_andopenfile(x, y, z, world_folder, block_id):
    region_x, region_z = convert_to_region_coords(x, z)
    region_file = f'{world_folder}/region/r.{region_x}.{region_z}.mca'

    chunk_list = open_mca(region_file)

    chunk_num = get_chunk_number(x, z)
    section = get_section(y)
    block_states = chunk_list[chunk_num]['sections'][section]['block_states']
    if 'data' in block_states:
        block_data = chunk_list[chunk_num]['sections'][section]['block_states']['data']
    else:
        chunk_list[chunk_num]['sections'][section]['block_states'] = new_block_states(block_id)
        block_data = chunk_list[chunk_num]['sections'][section]['block_states']['data']

    palette = chunk_list[chunk_num]['sections'][section]['block_states']['palette']

    palette_id = -1
    for i, block in enumerate(palette):
        if block['Name'].value == block_id:
            palette_id = i
    if palette_id == -1:
        new_block = TAG_Compound()
        new_block.tags.append(TAG_String(name='Name', value=block_id))
        palette_id = len(palette)
        palette.append(new_block)

    block_int_pos = find_int_num(x, y, z, 4)
    block_int = block_data[block_int_pos]

    pos_in_int = find_position_in_int(x, y, z, 4)

    new_block_int = replace_block_id(block_int, pos_in_int, 4, palette_id)

    # DANGER
    chunk_list[chunk_num]['sections'][section]['block_states']['data'][block_int_pos] = new_block_int
    os.remove(region_file)
    generate_mca(chunk_list, region_file)
    print(f'changed {x}, {y}, {z} to {block_id}')


def repack_blockdata(block_ints, prev_bits, new_bits):
    new_int_count = math.ceil((4096*new_bits)/(64-(64 % new_bits)))
    new_block_ints = [0]*new_int_count
    for i in range(0, 4096):
        block_id = extract_block(block_ints, prev_bits, i)
        replace_block_indata(new_block_ints, new_bits, i, block_id)
    return new_block_ints


def set_block(x, y, z, chunk_list, block_id):
    chunk_num = get_chunk_number(x, z)
    section = get_section(y)
    block_states = chunk_list[chunk_num]['sections'][section]['block_states']
    if 'data' in block_states:
        block_data = chunk_list[chunk_num]['sections'][section]['block_states']['data']
    else:
        chunk_list[chunk_num]['sections'][section]['block_states'] = new_block_states(block_id)
        block_data = chunk_list[chunk_num]['sections'][section]['block_states']['data']

    palette = chunk_list[chunk_num]['sections'][section]['block_states']['palette']

    palette_id = -1
    for i, block in enumerate(palette):
        if block['Name'].value == block_id:
            palette_id = i
    if palette_id == -1:
        new_block = TAG_Compound()
        new_block.tags.append(TAG_String(name='Name', value=block_id))
        palette_id = len(palette)
        chunk_list[chunk_num]['sections'][section]['block_states']['palette'].append(new_block)

    bits_per_block = math.ceil(math.log2(len(palette)+1))
    if bits_per_block < 4:
        bits_per_block = 4

    block_int_pos = find_int_num(x, y, z, bits_per_block)
    if block_int_pos >= len(block_data):
        block_data = repack_blockdata(block_data, bits_per_block-1, bits_per_block)

    chunk_offset = global_xyz_to_chunk_offset(x, y, z)

    replace_block_indata(block_data, bits_per_block, chunk_offset, palette_id)


def fill_column(x, y_bottom, y_top, z, chunk_list, block_list):
    for y in range(y_bottom, y_top+1):
        block_type = block_list[3]
        if y < ((y_top - y_bottom)/2) + y_bottom:
            block_type = block_list[1]
        else:
            block_type = block_list[2]
        if y == y_bottom:
            block_type = block_list[0]
        if y == y_top:
            block_type = block_list[3]
        set_block(x, y, z, chunk_list, block_type)


def fun_equation(x, z):
    return math.floor(30 * math.sin(x/10) * math.cos(z/10)) + 100


def read_csv_data(path):
    csv_data = []
    with open(path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in spamreader:
            csv_data.append([float(s) for s in row[0].split(',')])
    return csv_data


empty_region = []


def generate_new_chunks(region_x, region_z):
    print(f'generating {region_x, region_z}')
    new_region = []
    for i in range(0, 1024):
        new_region.append(copy_nbt_file(empty_region[i]))
        new_region[i]['xPos'].value, new_region[i]['zPos'].value = region_chunknum_to_chunk_coords(region_x, region_z, i)
    return new_region


def get_chunks(x, z, world, region_cache):
    region_x, region_z = convert_to_region_coords(x, z)
    filename = f'r.{region_x}.{region_z}.mca'
    if filename in region_cache:
        chunks = region_cache[filename]['chunks']
    else:
        region_file = f'{world}/region/{filename}'
        if exists(region_file):
            chunks = open_mca(region_file)
            for i in chunks:
                if i == 0:
                    chunks = generate_new_chunks(region_x, region_z)
                    break
        else:
            chunks = generate_new_chunks(region_x, region_z)
        region_cache[filename] = {'chunks': chunks, 'file': region_file}

    return chunks


def main():
    world_toedit = 'C:/Users/djhar/AppData/Roaming/.minecraft/saves/earth'
    world = world_toedit + '_edited'
    shutil.copytree(world_toedit, world)

    region_cache = {}

    earth = read_csv_data('topo_big.csv')

    block_list = ['minecraft:bedrock', 'minecraft:stone', 'minecraft:dirt', 'minecraft:grass_block']

    #for x in range(0, 1000):
    #    for z in range(0, 1000):
    #        height = fun_equation(x, z)
    #        chunks = get_chunks(x, z, world, region_cache)
    #        region_relative_x = x % 512
    #        region_relative_z = z % 512
    #        set_block(region_relative_x, height, region_relative_z, chunks, block_list[0])
            # set_block(0, -60, 0, chunks, 'minecraft:stone')

    global empty_region
    empty_region = copy_region(get_chunks(0, 0, world, region_cache))
    print(empty_region)

    for x in range(0, len(earth)*3):
        for z in range(0, len(earth[0])*3):
            val = earth[ (x/3).__floor__() ][ (z/3).__floor__() ]
            if val == 99999.0:
                continue
            height = (val / 100 + 105).__floor__()
            flipped_z = (1800*3) - z
            chunks = get_chunks(x, flipped_z, world, region_cache)
            region_relative_x = x % 512
            region_relative_z = flipped_z % 512
            fill_column(region_relative_x, 101, height, region_relative_z, chunks, block_list)

    for region in region_cache:
        chunks = region_cache[region]['chunks']
        filename = region_cache[region]['file']
        print(filename)
        if exists(filename):
            os.remove(filename)
        generate_mca(chunks, filename)


main()
# print_block_value(2, 74, 0, 'C:/Users/djhar/AppData/Roaming/.minecraft/saves/New World (2)')
