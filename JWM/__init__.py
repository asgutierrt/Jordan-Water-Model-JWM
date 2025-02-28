#    (c) Copyright 2021, Jim Yoon, Christian Klassert, Philip Selby,
#    Thibaut Lachaut, Stephen Knox, Nicolas Avisse, Julien Harou,
#    Amaury Tilmant, Steven Gorelick
#
#    This file is part of the Jordan Water Model (JWM).
#
#    JWM is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    JWM is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with JWM.  If not, see <http://www.gnu.org/licenses/>.

import os
import cPickle as pickle

global basepath
basepath = os.path.split(__file__)[0]
import numpy as np
import pandas as pd
from zipfile import ZipFile

global numpy_data_folder
numpy_data_folder = 'data/numpy_data'
global excel_data_folder
excel_data_folder = 'data/excel_data'
import shapefile

global shapefile_folder
shapefile_folder = 'data/shapefiles'

import logging

log = logging.getLogger(__name__)


def unzip_file(filename):
    """
    Unzip a file to the numpy data folder.

    """
    z = ZipFile(filename)
    z.extractall(os.path.join(basepath, numpy_data_folder))


def get_numpy_data(filename):
    """
    Takes the name of an npy file and returs the contents as a numpy array.
    """
    try:
        ddmatrix = np.load(os.path.join(basepath, numpy_data_folder, filename))
    except IOError:
        try:
            unzip_file(os.path.join(basepath, numpy_data_folder, "%s.zip" % filename))
            ddmatrix = np.load(os.path.join(basepath, numpy_data_folder, filename))
        except Exception, e:
            raise Exception("Error reading file %s. Error was: %s" % (filename, e))
    return ddmatrix


def get_excel_data(filename):
    """
        Return a pandas object of the given excel file. Looks by default in:
        ./excel_data/filename
    """
    # log.info("executing get_excel_data: " + basepath)
    # log.info("  ...cwd: " + os.getcwd())
    # log.info("  ...basepath: " + basepath)
    # log.info("  ...excel_data_folder: " + excel_data_folder)
    # log.info("  ...filename: " + filename)
    excel_data = pd.ExcelFile(os.path.join(basepath, excel_data_folder, filename))
    return excel_data


def get_data_file(filename):
    """
        Return a file object of the given file. Looks by default in:
        ./excel_data/filename
    """
    data_file = os.path.join(basepath, excel_data_folder, filename)

    if os.path.isfile(data_file) is False:
        return None

    with open(data_file, 'r') as f:
        data = f.read()

    return data


def write_data_file(filename, data):
    """
    Write data to the specified file. Will write to the excel_data folder.
    """
    data_file = os.path.join(basepath, excel_data_folder, filename)

    f = open(data_file, 'w+')

    f.write(data)


def get_pickle(filename):
    """
        Return a file object of the given file. Looks by default in:
        ./excel_data/filename
    """
    data_file = os.path.join(basepath, excel_data_folder, filename)

    if os.path.isfile(data_file) is False:
        return None

    data = pickle.load(open(data_file, "r"))

    return data


def write_pickle(filename, data):
    """
    Write data to the specified file. Will write to the excel_data folder.
    """
    data_file = os.path.join(basepath, excel_data_folder, filename)

    pickle.dump(data, open(data_file, "wb"))


def get_shapefile(filename):
    """
        Get a shapefile object from the given filenam -- looks autiomatically in
        ./shapefile/filename.
    """
    log.info("executing get_shapefile: " + basepath)
    log.info("  ...basepath: " + basepath)
    log.info("  ...shapefile_folder: " + shapefile_folder)
    log.info("  ...filename: " + filename)
    shp = shapefile.Reader(os.path.join(basepath, shapefile_folder, filename))
    shp_obj = shp.shapeRecords()
    return shp_obj


def get_df_from_pickle(model_dir, filename):
    """
            Get a pandas dataframe from pickle in specified path
    """
    filepath = os.path.join(model_dir, filename)
    if os.path.isfile(filepath):
        df = pd.read_pickle(filepath)
    else:
        log.warning(filepath + ' file not found')
        df = None
    return df