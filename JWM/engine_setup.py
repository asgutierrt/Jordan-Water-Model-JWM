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

"""This module includes functions to create pynsim engines using info from models_input.xlsx

Functions:
    create_engine()

"""

import engines

def create_engine(engine_class, target_name, simulator):
    """Create a pynsim engine and add to simulator object.

    This function is used by a simulator object (called in simulator's "set_engine"
    method. It uses information from model_setup.xlsx to create pynsim engines
    and adds engine to simulator (i.e. appends engine list, script in
    simulator's "set_engine" is relied upon to sequence the engines appropriately).

    Arguments:
        module_name (str): name of python module (i.e. filename in engines folder)
        engine_class (str): name of engine class
        target_name (str): name of target(s)
        engine_name (str): name of engine object
        simulator (pynsim simulator): pynsim simulator object

    Returns:
        (adds engine to pynsim simulator object)

    """

    EngineClass = getattr(engines, engine_class)

    if target_name == 'network':
        target = simulator.network
    else:
        target = simulator.network.get_institution(target_name)

    new_engine = EngineClass(target)
    simulator.add_engine(new_engine)

    engine_name = ""
    for l in range(len(engine_class)):
        if l == 0:
            engine_name += engine_class[l].lower()
        else:
            if engine_class[l].isupper():
                engine_name += "_" + engine_class[l].lower()
            else:
                engine_name += engine_class[l]

    simulator.engines[-1].name = engine_name

