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
#
#    This work product development was supported by the National Science
#    Foundation (NSF) under Grant GEO/OAD-1342869 and Grant ICER/EAR-1829999
#    (under the Belmont Forum Sustainable Urbanisation Global Initiative (SUGI)
#    / Food-Water-Energy Nexus theme). The NERC Belmont Forum
#    provided UK funding (NE/L009285/1). As part of the Belmont Forum,
#    the Deutsche Forschungsgemeinschaft (DFG) provided funding to the
#    Helmholtz Centre for Environmental Research (UFZ) (KL 2764/1-1)
#    and Leipzig University (GA 506/4-1), as well as the German Federal
#    Ministry of Education and Research (BMBF) funding to UFZ (033WU002).
#    The University of Manchester Computational Shared Facility is acknowledged.
#    Any opinions, findings, and conclusions or recommendations resulting from
#    in this product are solely those of the authors and do not necessarily
#    reflect the views of the NSF or other agencies that provided funding or data.


import os
import time
import simulation_setup
import pickle

start_time = time.time()

from JWM import basepath  # from jordanprototype import basepath

output_dir = os.path.join(basepath, 'outputs')
start_time = time.time()

# Setup list of simulations
simulations = simulation_setup.create_simulations()
simulation_setup.load_network(simulations)
simulation_setup.load_institutions(simulations)
simulation_setup.load_exogenous_inputs(simulations)

# Load observations from pickle file
data_file = os.path.join(basepath, 'data/pickle_data', 'observations.p')
f = open(data_file, 'r')
simulations[0].network.observations = pickle.load(f)
f.close()

# Load engines
simulation_setup.load_engines(simulations)

# Run each simulation in simulations list
s = simulations[0]
s.start()

end_time = time.time()
sim_time = end_time-start_time
print ("Simulation took:  %s" % sim_time)