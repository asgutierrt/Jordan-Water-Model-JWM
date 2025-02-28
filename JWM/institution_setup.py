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

"""This module instantiates institutions and loads them into a list.

This module instantiates the various Jordan institutions and loads them into a list. It also includes
methods to create institutions of special interest (e.g. all nodes of a certain type). The module uses institution
classes defined in components/institutions.py. The attributes and functions  defined in this module are primarily
called by methods in a JordanSimulator object (see simulator.py)

Attributes:
    institution_list (LIST): list of JordanInstitution objects

"""

#__author__ = 'jimyoon'

from JWM.components.institutions.government import JordanInstitution, CWA, JVA, WAJ, GOV, AllGOV, \
    AllNodesOfType
from JWM.components.institutions.market import \
    TankerMarket, HumanAgentWrapper, GovAgentWrapper, SubdistAgentWrapper

# create base institutions and consolidate into institution_list
# institution behaviors are defined in components/institutions.py
institution_list = []

# add high level institutions
institution_list.extend([CWA(name="cwa"), JVA(name="jva"), WAJ(name="waj")])

# add mid level institutions (Governorates)
institution_list.extend([GOV(name="balqa"), GOV(name="irbid"), GOV(name="ajloun"), GOV(name="jarash"),
                         GOV(name="mafraq"), GOV(name="zarqa"), GOV(name="amman"), GOV(name="madaba"),
                         GOV(name="karak"), GOV(name="maan"), GOV(name="tafilah"), GOV(name="aqaba")])

institution_list.append(JordanInstitution(name="jv_routing"))
institution_list.append(JordanInstitution(name="hl_routing"))

institution_list.append(TankerMarket(name="tanker_market"))
institution_list.append(HumanAgentWrapper(name="human_agent_wrapper"))
institution_list.extend([GovAgentWrapper(name="balqa_wrapper"), GovAgentWrapper(name="irbid_wrapper"),
                         GovAgentWrapper(name="ajloun_wrapper"), GovAgentWrapper(name="jarash_wrapper"),
                         GovAgentWrapper(name="mafraq_wrapper"), GovAgentWrapper(name="zarqa_wrapper"),
                         GovAgentWrapper(name="amman_wrapper"), GovAgentWrapper(name="madaba_wrapper"),
                         GovAgentWrapper(name="karak_wrapper"), GovAgentWrapper(name="maan_wrapper"),
                         GovAgentWrapper(name="tafilah_wrapper"), GovAgentWrapper(name="aqaba_wrapper")])
subdist_code_list = ['110101', '110201', '110301', '110401', '110501', '110601', '110701', '110702', '110801', '110802',
                     '110901', '110902', '110903', '120101', '120102', '120103', '120104', '120201', '120301', '120401',
                     '120501', '130101', '130102', '130103', '130104', '130201', '130301', '140101', '140102', '140103',
                     '140104', '140201', '140202', '140203', '210101', '210201', '210301', '210401', '210501', '210601',
                     '210701', '210801', '210901', '220101', '220102', '220103', '220104', '220201', '220202', '220203',
                     '220204', '220205', '220301', '220302', '220303', '220304', '220401', '230101', '230102', '230103',
                     '240101', '240102', '240103', '240201', '310101', '310201', '310202', '310301', '310302', '310401',
                     '310402', '310501', '310601', '310701', '320101', '320201', '320301', '330101', '330102', '330103',
                     '330104', '330105', '330201', '330301', '330401', '340101', '340102', '340201', '340202']
for i in subdist_code_list:
    institution_list.append(SubdistAgentWrapper(name=("subdist_" + i + "_wrapper")))


def get_institution_list():
    """Get list of Jordan institutions.

    Args:
        None

    Returns:
        institution_list (LIST): list of JordanInstitution objects

    """

    return institution_list


def get_all_gov_institutions(institution_list):
    """Get all institutions of type GOV from a list of institutions.

    Args:
        institution_list (LIST): list of institution objects

    Returns:
        all_govs (JordanInstitution object): JordanInstitution object containing all GOV institutions

    """

    all_govs = AllGOV(name="all_govs")
    for i in institution_list:
        if i.component_type == 'GOV':
            all_govs.add_institutions(i)
    return all_govs

def create_all_nodes_of_type_institutions(network):
    """Create institutions of specific node types.

    Args:
        network (pynsim network object): pynsim network object

    Returns:
        all_nodes_of_type_institutions (LIST): list containing institutions of all nodes of specific types

    """

    all_nodes_of_type_institutions = []

    all_nodes_of_type_institutions.append(AllNodesOfType('all_urban_households', 'UrbanHHAgent', network))
    all_nodes_of_type_institutions.append(AllNodesOfType('all_jv_farms', 'JVFarmAgentSimple', network))
    #all_nodes_of_type_institutions.append(AllNodesOfType('all_highland_farms', 'HighlandFarmAgentSimple', network))
    all_nodes_of_type_institutions.append(AllNodesOfType('all_highland_farms', 'HighlandFarmAgent', network))
    all_nodes_of_type_institutions.append(AllNodesOfType('all_gw_nodes', 'Groundwater', network))
    all_nodes_of_type_institutions.append(AllNodesOfType('all_reservoir_nodes', 'Reservoir', network))
    all_nodes_of_type_institutions.append(AllNodesOfType('all_wwtp_nodes', 'WWTP', network))

    return all_nodes_of_type_institutions


def add_govs_to_waj(network):
    """Add GOV institutions to WAJ institutiton.

    Args:
        network (pynsim network object): pynsim network object

    Returns:
        (adds GOV institutions to WAJ institution).

    """

    waj = network.get_institution('waj')
    for i in network.institutions:
        if i.component_type == 'GOV':
            waj.add_institutions(i)


def add_gov_wrappers_to_human_wrapper(network):
    """Add gov_agent_wrapper institutions to human_agent_wrapper institutiton.

    Args:
        network (pynsim network object): pynsim network object

    Returns:
        (adds GOV institutions to WAJ institution).

    """

    haw = network.get_institution('human_agent_wrapper')
    gov_wrappers = ["ajloun_wrapper", "amman_wrapper", "aqaba_wrapper", "balqa_wrapper", "irbid_wrapper",
                    "jarash_wrapper", "karak_wrapper", "maan_wrapper", "madaba_wrapper", "mafraq_wrapper",
                    "tafilah_wrapper", "zarqa_wrapper"]
    for i in network.institutions:
        if i.component_type == 'GovAgentWrapper':
            haw.add_institutions(i)
            haw.govs.append(i)
        if i.component_type == 'SubdistAgentWrapper':
            haw.add_institutions(i)
            haw.subdists.append(i)
            network.get_institution(gov_wrappers[i.gov-1]).add_institutions(i)

