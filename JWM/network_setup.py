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

"""This module includes attributes and functions to set up a pynsim network from shapefile and excel inputs.

Attributes:
    ddmatrix (numpy array): numpy array (249 x 249 x 600) of unit drawdown responses
    baseline_heads (numpy array): numpy array (249 x 600) of baseline head values
    baseline_pumping (numpy array): numpy array (249 x 600) of baseline pumping values
    head (numpy array): numpy array (249 x 600) of calculated head values
    gw_nodes (LIST): list of groundwater node objects
    network_nodes (LIST): list of network node objects
    urb_nodes (LIST): list of urban (household) node objects
    hfarm_nodes (LIST): list of highland farm objects
    jvfarm_nodes (LIST): list of jordan valley farm objects
    network_links (LIST): list of network link objects
    gw_network_links (LIST): list of groundwater node-network node link objects
    urb_network_links (LIST): list of urban (household) node-network node link objects
    hfarm_gw_links (LIST): list of highland farm node-groundwater node link objects
    jvfarm_network_links (LIST): list of jordan valley farm node-network node link objects

Functions:
    load_groundwater_data()
    load_network()
    read_res_properties()
    read_highland_farm_properties()
    read_jv_farm_propertes()
    read_urban_hh_properties()
    get_gw_nodes()
    get_network_nodes()
    get_urb_nodes()
    get_hfarm_nodes()
    et_jvfarm_nodes()
    get_network_links()
    get_gw_network_links()
    get_urb_network_links()
    get_hfarm_gw_links()
    get_jvfarm_network_links()

"""

from math import isnan
from JWM.components.nodes.network_nodes import Groundwater, Reservoir, Junction, AgDemand, UrbDemand, WTP, WWTP
from JWM.components.nodes.urban_nodes import UrbanHHAgent, HumanHHAgent, HumanCOAgent, HumanRFAgent, HumanINAgent
from JWM.components.nodes.farm_nodes import HighlandFarmAgent, JVFarmAgentSimple
from JWM.components.links.links import River, Pipeline, GW_Pipeline, KAC, Return, Nearest
from JWM import get_numpy_data, get_shapefile, get_excel_data, get_df_from_pickle
from JWM import basepath

import pandas as pd
pd.set_option('display.width', 320)
import numpy as np
import os
import logging
log = logging.getLogger(__name__)
import datetime

global ddmatrix
ddmatrix=None
global baseline_heads
baseline_heads=None
global baseline_pumping
baseline_pumping=None
global head
head=None
global gw_nodes
gw_nodes = []
global network_nodes
network_nodes = []
global ww_nodes
ww_nodes = []
global urb_nodes
urb_nodes = []
global hfarm_nodes
hfarm_nodes = []
global jvfarm_nodes
jvfarm_nodes = []
global network_links
network_links = []
global gw_network_links
gw_network_links = []
global urb_network_links
urb_network_links = []
global hfarm_gw_links
hfarm_gw_links = []
global jvfarm_network_links
jvfarm_network_links = []
global human_hh_agents
human_hh_agents = []
global human_co_agents
human_co_agents = []
global human_rf_agents
human_rf_agents = []
global human_in_agents
human_in_agents = []

def load_groundwater_data():
    """Loads groundwater response matrix data from numpy files (in numpy_data folder)."""

    global ddmatrix
    global baseline_heads
    global baseline_pumping
    global head

    ddmatrix_pt1 = get_numpy_data('ddmatrix_extended_pt1.npy')  # 249 x 249 x 600 numpy array
    ddmatrix_pt2 = get_numpy_data('ddmatrix_extended_pt2.npy')  # 249 x 249 x 600 numpy array
    ddmatrix = np.concatenate((ddmatrix_pt1, ddmatrix_pt2))
    baseline_heads = get_numpy_data('baseline_heads_extended.npy')  # 249 x 600 numpy array
    baseline_pumping = get_numpy_data('baseline_pumping.npy')  # 249 x 600 numpy array
    head = baseline_heads  # 249 x 600 numpy array


def load_network(gw_nodes_shpname, network_nodes_shpname, network_links_shpname, urban_households_shpname,
    highland_farms_shpname, jv_farms_shpname, ww_nodes_shpname):
    """Load nodes and links as pynsim objects from shapefiles (in shapefiles folder).

    Uses features in shapefiles and inputs stored in shapefile attribute tables to generate pynsim
    node and link objects.

    Args:
        gw_nodes_shpname (str): groundwater nodes shapefile name
        network_nodes_shpname (str): network nodes shapefile name
        network_links_shpname (str): network links shapefile name
        urban_households_shpname (str): urban household nodes shapefile name
        highland_farms_shpname (str): highland farmer nodes shapefile name

    """

    global gw_nodes
    global network_nodes
    global ww_nodes
    global urb_nodes
    global hfarm_nodes
    global jvfarm_nodes
    global network_links
    global gw_network_links
    global urb_network_links
    global hfarm_gw_links
    global jvfarm_network_links

    # Read in groundwater nodes from shapefile
    gw_nodes_obj = get_shapefile(gw_nodes_shpname) # creates list of shape/record objects for each point in shapefile
    count = 0

    for i in gw_nodes_obj:
        subdist = str(i.record[0])[0:6]
        l = i.record[1]
        if l == 12:
            lyr = str(i.record[1])[0:2]
        else:
            lyr = '0' + str(i.record[1])[0:1]
        type = i.record[3]
        node_name = subdist+"_"+type+"_"+lyr
        gw_nodes.append(Groundwater(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))

        # assign groundwater node properties
        gw_nodes[count]._gw_node_properties = {}

        gw_nodes[count]._gw_node_properties['use_type'] = i.record[3]
        gw_nodes[count]._gw_node_properties['baseline_pumping'] = i.record[2]
        gw_nodes[count]._gw_node_properties['pumping'] = i.record[2]  # default pumping input is baseline value
        gw_nodes[count]._gw_node_properties['pumping_projection'] = [i.record[2]] * 600  # default pumping projection is time series of baseline value
        gw_nodes[count]._gw_node_properties['subdistrict'] = subdist
        gw_nodes[count]._gw_node_properties['baseline_heads'] = baseline_heads[int(i.record[13])]
        gw_nodes[count]._gw_node_properties['head_calc'] = baseline_heads[int(i.record[13])]
        gw_nodes[count]._gw_node_properties['swl'] = i.record[14] # uses estimated swl in 2006 for historical model run
        gw_nodes[count]._gw_node_properties['inwell_dd'] = i.record[5]
        gw_nodes[count]._gw_node_properties['bot'] = i.record[6]
        gw_nodes[count]._gw_node_properties['top'] = i.record[7]
        gw_nodes[count]._gw_node_properties['gov'] = i.record[10]
        gw_nodes[count]._gw_node_properties['matrix_position'] = i.record[13]
        gw_nodes[count]._gw_node_properties['bias_correction'] = i.record[14] - baseline_heads[int(i.record[13])][0] # uses estimated swl in 2006 for historical model run
        gw_nodes[count]._gw_node_properties['tds'] = i.record[11]
        gw_nodes[count]._gw_node_properties['basin'] = i.record[15]

        gw_nodes[count].institution_names = ['WAJ']

        count = count + 1

    # Read in network nodes from shapefile
    network_nodes_obj = get_shapefile(network_nodes_shpname)
    network_nodes = []
    count = 0
    for i in network_nodes_obj:
        type = i.record[1]
        node_name = i.record[2]
        if type == 'res':
            network_nodes.append(Reservoir(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        if type == 'jnc':
            network_nodes.append(Junction(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        if type == 'ag':
            # create separate ag demand network node and JV farm agent node at same location (decide how to implement)
            network_nodes.append(AgDemand(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        if type == 'urb':
            tempnode=UrbDemand(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name)
            network_nodes.append(tempnode)
        if type == 'tp':
            network_nodes.append(WTP(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        if type == 'wwtp':
            network_nodes.append(WWTP(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        # load any additional properties of node defined in shapefile
        network_nodes[count].institution_names = str(i.record[4]).replace(" ", "").split(',')
        network_nodes[count].routing_engine = str(i.record[5]).replace(" ", "").split(',')
        network_nodes[count].node_type = i.record[1]

        count = count + 1

    # Read in wastewater nodes from shapefile
    ww_nodes_obj = get_shapefile(ww_nodes_shpname)
    ww_nodes = []
    count = 0
    for i in ww_nodes_obj:
        type = i.record[1]
        node_name = i.record[2]
        ww_nodes.append(WWTP(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        # load any additional properties of node defined in shapefile
        ww_nodes[count].institution_names = str(i.record[4]).replace(" ", "").split(',')
        ww_nodes[count].routing_engine = str(i.record[5]).replace(" ", "").split(',')
        ww_nodes[count].node_type = i.record[1]
        ww_nodes[count].capacity = i.record[7]
        ww_nodes[count].effluent_dest = str(i.record[6]).replace(" ", "").split(',')
        ww_nodes[count].served_govs = str(i.record[8]).replace(" ", "").split(',')
        ww_nodes[count].dest1_percent = i.record[9]
        ww_nodes[count].dest2_percent = i.record[10]
        ww_nodes[count].gov1_mcm_2012 = i.record[11]
        ww_nodes[count].gov2_mcm_2012 = i.record[12]

        count = count + 1

    # Read in network links from shapefile
    network_links_obj = get_shapefile(network_links_shpname)
    network_links = []
    count = 0
    for i in network_links_obj:
        link_type = i.record[1]
        link_name = i.record[2]
        link_start = i.record[3]
        link_end = i.record[4]
        for start in network_nodes:
            for end in network_nodes:
                if start.name == link_start and end.name == link_end:
                    if link_type == 'river':

                        templink = River(start_node=start,end_node=end,name=link_name)
                        templink.linktype='River'
                        network_links.append(templink)

                    if link_type == 'pipeline':
                        templink = Pipeline(start_node=start,end_node=end,name=link_name)
                        templink.linktype='Pipeline'
                        templink.diameter=i.record[8]
                        templink.multiplier=i.record[9]
                        templink.Vmax=i.record[10]
                        templink.max_flow=i.record[14]
                        templink.cost=i.record[12]

                        network_links.append(templink)
                        if link_name=='amman_zarqa':
                            link_amman_zarqa=templink
                        elif link_name == 'zarqa_mafraq':
                            link_zarqa_mafraq=templink
                        elif link_name == 'madaba':
                            link_madaba = templink
                        elif link_name == 'karak':
                            link_karak=templink
                        elif link_name == 'mafraq_irbid':
                            link_mafraq_irbid=templink
                                #----------------------

                    if link_type == 'kac':
                        templink = KAC(start_node=start,end_node=end,name=link_name)
                        templink.linktype='KAC'
                        network_links.append(templink)

                    if link_type == 'return':
                        templink = Return(start_node=start,end_node=end,name=link_name)
                        templink.linktype='Return'
                        network_links.append(templink)
        # load any additional properties of link defined in shapefile
        network_links[count].institution_names = str(i.record[6]).replace(" ", "").split(',')
        network_links[count].routing_engine = str(i.record[7]).replace(" ", "").split(',')
        network_links[count].length = i.record[13]

        count = count + 1

    zarqa_node=link_amman_zarqa.end_node
    amman_node=link_amman_zarqa.start_node
    zarqa_amman = Pipeline(start_node=zarqa_node,end_node=amman_node,name='zarqa_amman')
    zarqa_amman.linktype='Pipeline'
    zarqa_amman.diameter=link_amman_zarqa.diameter
    zarqa_amman.multiplier=link_amman_zarqa.multiplier
    zarqa_amman.Vmax=link_amman_zarqa.Vmax
    zarqa_amman.max_flow=link_amman_zarqa.max_flow
    zarqa_amman.cost=link_amman_zarqa.cost
    zarqa_amman.institution_names = link_amman_zarqa.institution_names
    zarqa_amman.routing_engine = link_amman_zarqa.routing_engine
    network_links.append(zarqa_amman)

    mafraq_node=link_zarqa_mafraq.end_node
    mafraq_zarqa = Pipeline(start_node=mafraq_node,end_node=zarqa_node,name='mafraq_zarqa')
    mafraq_zarqa.linktype='Pipeline'
    mafraq_zarqa.diameter=link_zarqa_mafraq.diameter
    mafraq_zarqa.multiplier=link_zarqa_mafraq.multiplier
    mafraq_zarqa.Vmax=link_zarqa_mafraq.Vmax
    mafraq_zarqa.max_flow=link_zarqa_mafraq.max_flow
    mafraq_zarqa.cost=link_zarqa_mafraq.cost
    mafraq_zarqa.institution_names = link_zarqa_mafraq.institution_names
    mafraq_zarqa.routing_engine = link_zarqa_mafraq.routing_engine
    network_links.append(mafraq_zarqa)

    madaba_node=link_madaba.end_node
    madaba_jnc=link_madaba.start_node
    from_madaba = Pipeline(start_node=madaba_node,end_node=madaba_jnc,name='from_madaba')
    from_madaba.linktype='Pipeline'
    from_madaba.diameter=link_madaba.diameter
    from_madaba.multiplier=link_madaba.multiplier
    from_madaba.Vmax=link_madaba.Vmax
    from_madaba.max_flow=link_madaba.max_flow
    from_madaba.cost=link_madaba.cost
    from_madaba.institution_names = link_madaba.institution_names
    from_madaba.routing_engine = link_madaba.routing_engine
    network_links.append(from_madaba)

    karak_node=link_karak.end_node
    karak_knc =link_karak.start_node
    from_karak = Pipeline(start_node=karak_node,end_node=karak_knc,name='from_karak')
    from_karak.linktype='Pipeline'
    from_karak.diameter=link_karak.diameter
    from_karak.multiplier=link_karak.multiplier
    from_karak.Vmax=link_karak.Vmax
    from_karak.max_flow=link_karak.max_flow
    from_karak.cost=link_karak.cost
    from_karak.institution_names = link_karak.institution_names
    from_karak.routing_engine = link_karak.routing_engine  
    network_links.append(from_karak)  

    irbid_node=link_mafraq_irbid.end_node
    irbid_mafraq = Pipeline(start_node=irbid_node,end_node=mafraq_node,name='irbid_mafraq')
    irbid_mafraq.linktype='Pipeline'
    irbid_mafraq.diameter=link_mafraq_irbid.diameter
    irbid_mafraq.multiplier=link_mafraq_irbid.multiplier
    irbid_mafraq.Vmax=link_mafraq_irbid.Vmax
    irbid_mafraq.max_flow=link_mafraq_irbid.max_flow
    irbid_mafraq.cost=link_mafraq_irbid.cost
    irbid_mafraq.institution_names = link_mafraq_irbid.institution_names
    irbid_mafraq.routing_engine = link_mafraq_irbid.routing_engine
    network_links.append(irbid_mafraq)

    # Create links to connect urban groundwater nodes to urban demand network nodes
    gw_network_links = []
    count = 0

    for g in range(len(gw_nodes)):
        if gw_nodes[g]._gw_node_properties['use_type'] == 'urb':
            link_name = gw_nodes[g].name + "_" + gw_nodes_obj[g].record[10]
            for end in network_nodes:
                if gw_nodes_obj[g].record[10] == end.name:
                    templink=GW_Pipeline(start_node=gw_nodes[g], end_node=end, name=link_name)
                    templink.linktype='GW_Pipeline'
                    gw_network_links.append(templink)
                    # add any additional properties
                    gw_network_links[count].institution_names = ['WAJ']
                    count = count + 1

    # Create urban household agents as nodes #
    urb_nodes_obj = get_shapefile(urban_households_shpname)

    urb_nodes = []
    count = 0
    for i in urb_nodes_obj:
        node_name = str(i.record[5])[0:6] + "_urb"
        gov_institution = 'gov_' + i.record[33][0:3]
        urb_nodes.append(UrbanHHAgent(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        # add any additional properties
        urb_nodes[count].node_type = 'urb_demand'
        urb_nodes[count].subdistrict = str(i.record[5])[0:6]
        urb_nodes[count].urbnode = i.record[33]
        urb_nodes[count].institution_names = ['WAJ', gov_institution]
        count = count + 1

    # Create links to connect urban household agents to nearest urban network node
    urb_network_links = []
    for u in range(len(urb_nodes)):
        link_name = urb_nodes[u].name + "_" + urb_nodes_obj[u].record[33]
        for end in network_nodes:
            if end.name == urb_nodes_obj[u].record[33]:
                urb_network_links.append(Nearest(start_node=urb_nodes[u], end_node=end, name=link_name))
        # add any additional properties
        urb_network_links[u].institution_names = ['WAJ']

    # Create highland ag agents (only create agents for sub-districts with associated ag wells) #
    hfarm_nodes_obj = get_shapefile(highland_farms_shpname)
    hfarm_nodes = []
    hfarm_gw_links = []

    # Add farm nodes #
    log.info('ADD HIGHLAND FARM NODES')
    for i in hfarm_nodes_obj:
        node_prefix = str(i.record[5])[0:6]
        node_name = node_prefix + "_hfarm"
        hfarm_nodes.append(HighlandFarmAgent(x=i.shape.points[0][0], y=i.shape.points[0][1], name=node_name))
        hfarm_nodes[-1].subdistrict = node_prefix
        hfarm_nodes[-1].institution_names = ['WAJ']

    for i in hfarm_nodes:
        for g in gw_nodes:
            if i.name[0:6] + "_ag" == g.name[0:9]:
                link_name = i.name + "__" + g.name
                hfarm_gw_links.append(Nearest(start_node=g, end_node=i, name=link_name))
                hfarm_gw_links[-1].institution_names = ['WAJ']
                hfarm_gw_links[-1].subdistrict = i.subdistrict

    # Add percentage contribution of each groundwater node in terms of pumping as attribute to highland farms
    for hfarm in hfarm_nodes:
        hfarm.gw_percentage = {}
        pumping_sum = 0
        count = 0
        for g in hfarm.upstream_nodes:
            pumping_sum += g._gw_node_properties['baseline_pumping']
        for g in hfarm.upstream_nodes:
            hfarm.gw_percentage[g.name] = g._gw_node_properties['baseline_pumping'] / pumping_sum

    log.info('End Highland Farm Network Setup')

#######################################################################################################################
#######################################################################################################################

    #  Create Simple Jordan Valley Farm Agents
    log.info('Set up Jordan Valley Farm Agents')
    jvfarm_nodes_obj = get_shapefile(jv_farms_shpname)

    #  Create jordan valley farm agents
    jv_farm_xlsx = get_excel_data("jv_farms_da.xlsx")
    jv_farm_data = jv_farm_xlsx.parse("da_data")
    jvfarm_nodes = []
    count = 0
    for i in jvfarm_nodes_obj:
        da = i.record[2]
        if da in jv_farm_data.da.values:
            # for prototype, create jv farmer agent at same location as jv ag network demand nodes
            jvfarm_nodes.append(JVFarmAgentSimple(x=i.shape.points[0][0], y=i.shape.points[0][1], name=str(da) + '_jvfarm'))
            # load any additional properties of node defined in shapefile
            jvfarm_nodes[count].da = i.record[2]
            jvfarm_nodes[count].so1 = i.record[15][0]
            jvfarm_nodes[count].so2 = i.record[16][0]
            jvfarm_nodes[count].so_split = i.record[17]
            jvfarm_nodes[count].so_mwi = i.record[23]
            jvfarm_nodes[count].farmed_du = i.record[8]
            jvfarm_nodes[count].sml_du = i.record[9]
            jvfarm_nodes[count].med_du = i.record[10]
            jvfarm_nodes[count].lrg_du = i.record[11]
            # Get DA data from excel file
            jvfarm_nodes[count].crop_areas_2015 = {}
            jvfarm_nodes[count].crop_areas_2015['cit'] = jv_farm_data[(jv_farm_data.da == da)]['cit'].values[0]
            jvfarm_nodes[count].crop_areas_2015['ban'] = jv_farm_data[(jv_farm_data.da == da)]['ban'].values[0]
            jvfarm_nodes[count].crop_areas_2015['veg'] = jv_farm_data[(jv_farm_data.da == da)]['veg'].values[0]
            jvfarm_nodes[count].crop_areas_2015['dat'] = jv_farm_data[(jv_farm_data.da == da)]['dat'].values[0]
            jvfarm_nodes[count].jv_region = jv_farm_data[(jv_farm_data.da == da)]['region'].values[0]
            jvfarm_nodes[count].farm_type = jv_farm_data[(jv_farm_data.da == da)]['iwmi_category'].values[0]
            jvfarm_nodes[count].cit_farm_type = jv_farm_data[(jv_farm_data.da == da)]['cit_type'].values[0]
            jvfarm_nodes[count].ban_farm_type = jv_farm_data[(jv_farm_data.da == da)]['ban_type'].values[0]
            jvfarm_nodes[count].veg_farm_type = jv_farm_data[(jv_farm_data.da == da)]['veg_type'].values[0]
            jvfarm_nodes[count].dat_farm_type = jv_farm_data[(jv_farm_data.da == da)]['dat_type'].values[0]
            jvfarm_nodes[count].institution_names = ['JVA']
            count = count + 1

    #  Create links to connect jordan valley farm agents to network ag demand node
    jvfarm_network_links = []
    count = 0
    for j in jvfarm_nodes:
        if j.so1 <> '0':
            for n in network_nodes:
                if n.component_type == 'AgDemand':
                    if len(n.name) == 4:
                        n_da = n.name[2:4]
                    else:
                        n_da = n.name[2:3]
                    if str(j.so1) == n_da:
                        link_name = j.name + '_' + n.name
                        jvfarm_network_links.append(Nearest(start_node=n, end_node=j, name=link_name))
                        jvfarm_network_links[count].institution_names = ['JVA']
                        count = count + 1
        if j.so2 <> '0':
            for n in network_nodes:
                if n.component_type == 'AgDemand':
                    if len(n.name) == 4:
                        n_da = n.name[2:4]
                    else:
                        n_da = n.name[2:3]
                    if str(j.so2) == n_da:
                        link_name = j.name + '_' + n.name
                        jvfarm_network_links.append(Nearest(start_node=n, end_node=j, name=link_name))
                        jvfarm_network_links[count].institution_names = ['JVA']
                        count = count + 1
    log.info('Completed Jordan Valley Farm Agent Setup')


def read_res_properties(nodes):  # Read in reservoir properties and initial storage levels
    """Add reservoir parameters to reservoir nodes in network.

    Loads data from res_properties.xlsx as a pandas dataframe and sets appropriate parameters for reservoir nodes.

    Args:
        nodes (LIST): list of pynsim nodes

    Returns:
        (updates reservoir node properties)

    """
    res_properties_xlsx = get_excel_data("res_properties.xlsx")
    res_properties = res_properties_xlsx.parse("properties")
    for n in nodes:
        if n.component_type == 'Reservoir':
            n.res_properties = {}
            for key in n.res_property_indices:
                if res_properties[(res_properties.Property == key) & (res_properties.Reservoir == n.name)]['Value'].any():
                    n.res_properties[key] = res_properties[(res_properties.Property == key) & (res_properties.Reservoir == n.name)]['Value'].values[0]
                else:
                    n.res_properties[key] = 'NA'

def read_highland_farm_properties(nodes):
    pass


def read_jv_farm_propertes(nodes):
    pass


def read_urban_hh_properties(nodes):
    pass


def load_human_agents(human_module_inputs):
    """Load household and commercial agents. These are basic PyNSim components.

    Args:
        human_module_inputs  (LIST): Several excel file sheets containing all agent initialization parameters.

    """

    global human_hh_agents
    global human_co_agents
    global human_rf_agents
    global human_in_agents

    log.info('Load urban module data')
    human_hh_agents = []
    human_co_agents = []
    human_rf_agents = []
    human_in_agents = []
    hh_global_params = human_module_inputs.parse("hh_global_params")[0:56]
    hh_df_params = human_module_inputs.parse("hh_df_params")[0:15]
    hh_supply_durations = human_module_inputs.parse("hh_supply_durations")[0:89]
    hh_subdistricts = human_module_inputs.parse("hh_subdistricts")[0:89]
    hh_month_factors = human_module_inputs.parse("hh_seasonality")[0:12]

    hh_storage = hh_global_params["storage"][0]
    hh_tanker_prices = hh_global_params["exog_tanker_price"]
    hh_sewage_rates = hh_global_params["sewage_rate"][0:12]
    hh_subdist_code = hh_subdistricts["subdist_code"][0:89]
    hh_subdist_name = hh_subdistricts["subdist_name"][0:89]
    hh_subdist_gov = hh_subdistricts["gov_code"][0:89]
    hh_gov_name = hh_subdistricts["gov_name"][0:89]
    count = 0
    for h in hh_gov_name:
        hh_gov_name[count] = h.lower()[0:3]
        count += 1
    hh_subdist_x = hh_subdistricts["x_coord"][0:89]
    hh_subdist_y = hh_subdistricts["y_coord"][0:89]
    hh_subdist_pop = hh_subdistricts["population"][0:89]
    hh_subdist_gov_pop = hh_subdistricts["gov_pop"][0:89]
    rf_supply_durations = human_module_inputs.parse("rf_supply_durations")[0:89]
    rf_subdistricts = human_module_inputs.parse("rf_subdistricts")[0:890]
    rf_subdist_pop = rf_subdistricts["population"][0:890]
    co_global_params = human_module_inputs.parse("co_global_params")[0:60]
    co_df_params = human_module_inputs.parse("co_df_params")[0:12]
    co_populations = human_module_inputs.parse("co_populations")[0:5]
    co_supply_durations = human_module_inputs.parse("co_supply_durations")[0:89]
    co_sizes = co_global_params["size_category"][0:5]
    co_connection_rates = co_global_params["connection_rate"]
    co_sewage_rates = co_global_params["sewage_rate"]
    co_summer_months = co_global_params["summer_months"]
    co_tanker_prices = co_global_params["exog_tanker_price"]
    co_connection_size_factors = co_global_params["connection_size_factor"]
    in_global_params = human_module_inputs.parse("in_global_params")[0:12]
    in_subdistricts = human_module_inputs.parse("in_subdistricts")[0:89]
    in_tariff = in_global_params["policy_params"][0]
    in_groundwater_cost = in_global_params["groundwater_cost"][0]
    in_surface_water_cost = in_global_params["surface_water_cost"][0]
    in_month_factors = in_global_params["seasonality_factor"]
    in_subdist_code = in_subdistricts["subdist_code"][0:89]
    in_subdist_name = in_subdistricts["subdist_name"][0:89]
    in_subdist_gov = in_subdistricts["gov_code"][0:89]
    in_gov_name = in_subdistricts["gov_name"][0:89]
    count = 0
    for i in in_gov_name:
        in_gov_name[count] = i.lower()[0:3]
        count += 1
    in_subdist_x = in_subdistricts["x_coord"][0:89]
    in_subdist_y = in_subdistricts["y_coord"][0:89]
    in_subdist_has_industry = in_subdistricts["has_industry"][0:89]
    in_subdist_is_active = in_subdistricts["is_active"][0:89]
    in_subdist_industry_name = in_subdistricts["industry_name_long"][0:89]
    in_subdist_piped_water_use = in_subdistricts["piped_water_use"][0:89]
    in_subdist_well_water_use = in_subdistricts["well_water_use"][0:89]
    in_subdist_groundwater_node = in_subdistricts["groundwater_node"][0:89]
    in_subdist_surface_water_use = in_subdistricts["surface_water_use"][0:89]
    in_subdist_water_value_per_m3 = in_subdistricts["water_value_per_m3"][0:89]
    in_subdist_water_source_names = in_subdistricts["water_source_names"][0:89]
    gov_wrappers = ["ajloun_wrapper", "amman_wrapper", "aqaba_wrapper", "balqa_wrapper", "irbid_wrapper",
                    "jarash_wrapper", "karak_wrapper", "maan_wrapper", "madaba_wrapper", "mafraq_wrapper",
                    "tafilah_wrapper", "zarqa_wrapper"]

    log.info('Set up Urban Water User Agents')
    count = 0
    for subdist in range(1, 90):
        subdist_code = hh_subdist_code[subdist-1]
        gov = hh_subdist_gov[subdist-1]
        gov_name = hh_gov_name[subdist - 1]
        subdist_hnum = float(hh_subdistricts.ix[(subdist-1), "hnum"])
        no_of_agents = int(hh_supply_durations.ix[(subdist-1), "no_of_agents"])
        for duration_class in range(no_of_agents):
            duration_pop_weight = float(hh_supply_durations.ix[(subdist - 1), ("weight"+str(duration_class+1))])
            duration_avg_hours = float(hh_supply_durations.ix[(subdist - 1), ("duration"+str(duration_class+1))])
            represented_units_base = duration_pop_weight * float(hh_subdist_pop[subdist - 1]) / subdist_hnum
            if not isnan(represented_units_base):
                for is_sewage in [True,False]:
                    sewage_pop_weight = (hh_sewage_rates[gov-1] if is_sewage else (1 - hh_sewage_rates[gov-1]))
                    represented_units = represented_units_base * sewage_pop_weight
                    name = 'hh_agent_' + str(subdist_code) + '_' + str(duration_class+1) + '_' + \
                           ("sewage" if is_sewage else "no-swg") + str(count)
                    coefs = hh_df_params[0].tolist()
                    params = hh_subdistricts.ix[(subdist-1), "constant":]
                    month_factors = hh_month_factors[gov]
                    human_hh_agents.append(HumanHHAgent(name, gov, gov_name, (subdist-1), subdist_code, subdist,
                                                        str(hh_subdist_name[subdist-1]), hh_subdist_x[subdist-1],
                                                        hh_subdist_y[subdist-1], represented_units, coefs,
                                                        params, duration_avg_hours, duration_pop_weight, hh_storage,
                                                        month_factors, hh_tanker_prices, is_sewage,
                                                        sewage_pop_weight))
                    human_hh_agents[count].institution_names = ['human_agent_wrapper', gov_wrappers[gov-1],
                                                                ("subdist_" + str(subdist_code) + "_wrapper")]
                    count += 1
    count = 0
    for subdist in range(1, 90):
        subdist_code = hh_subdist_code[subdist-1]
        gov = int(hh_subdist_gov[subdist-1])
        gov_name = hh_gov_name[subdist-1]
        gov_populations = co_populations[gov]
        no_of_agents = int(co_supply_durations.ix[(subdist-1), "no_of_agents"])
        for size_class in range(len(co_sizes)):
            size = co_sizes[size_class]
            represented_units_base = float(gov_populations[size_class])
            represented_units_factor = float(hh_subdist_pop[subdist-1]) / float(hh_subdist_gov_pop[subdist-1])
            represented_units_base *= represented_units_factor
            for duration_class in range(no_of_agents):
                duration_pop_weight = float(co_supply_durations.ix[(subdist - 1), ("weight" + str(duration_class+1))])
                duration_avg_hours = float(co_supply_durations.ix[(subdist - 1), ("size_class_" + str(int(size)) + "_" +
                                                                                  str(duration_class+1))])
                represented_units = duration_pop_weight * represented_units_base
                if not isnan(represented_units):
                    name = 'co_agent_' + str(subdist_code) + '_' + str(size) + '_' + str(duration_class+1) + str(count)
                    coefs = co_df_params[0].tolist()
                    params = co_df_params[size_class+1].tolist()
                    connection_rate = co_connection_rates[size_class]
                    sewage_rate = co_sewage_rates[size_class]
                    co_connection_size_factor = co_connection_size_factors[size_class]
                    human_co_agents.append(HumanCOAgent(name, gov, gov_name, size_class, subdist_code, subdist,
                                                        str(hh_subdist_name[subdist-1]), hh_subdist_x[subdist-1],
                                                        hh_subdist_y[subdist-1], represented_units,
                                                        coefs, params, duration_avg_hours, duration_pop_weight,
                                                        connection_rate, sewage_rate, co_summer_months,
                                                        co_tanker_prices, co_connection_size_factor))
                    human_co_agents[count].institution_names = ['human_agent_wrapper', gov_wrappers[gov-1],
                                                                ("subdist_" + str(subdist_code) + "_wrapper")]
                    count += 1
    count = 0
    for subdist in range(1, 90):
        is_camp_location = True
        subdist_code = hh_subdist_code[subdist-1]
        subdist_2011_row = ((2011 - 2006) * 89) + (subdist-1)
        gov = hh_subdist_gov[subdist-1]
        gov_name = hh_gov_name[subdist-1]
        subdist_hnum = float(rf_subdistricts.ix[subdist_2011_row, "hnum"])
        no_of_agents = int(rf_supply_durations.ix[(subdist-1), "no_of_agents"])
        for duration_class in range(no_of_agents):
            duration_pop_weight = float(rf_supply_durations.ix[(subdist - 1), ("weight" + str(duration_class+1))])
            duration_avg_hours = float(rf_supply_durations.ix[(subdist - 1), ("duration" + str(duration_class+1))])
            represented_units_base = duration_pop_weight * float(rf_subdist_pop[subdist_2011_row]) / subdist_hnum
            if not isnan(represented_units_base):
                for is_sewage in [True,False]:
                    sewage_pop_weight = (hh_sewage_rates[gov-1] if is_sewage else (1 - hh_sewage_rates[gov-1]))
                    represented_units = represented_units_base * sewage_pop_weight
                    name = 'rf_agent_' + str(subdist_code) + '_' + str(duration_class+1) + '_' + \
                           ("sewage" if is_sewage else "no-swg") + str(count)
                    coefs = hh_df_params[0].tolist()
                    params = rf_subdistricts.ix[(subdist-1), "constant":]
                    month_factors = hh_month_factors[gov]
                    human_rf_agents.append(HumanRFAgent(name, gov, gov_name, (subdist-1), subdist_code, subdist,
                                                        str(hh_subdist_name[subdist-1]), hh_subdist_x[subdist-1],
                                                        hh_subdist_y[subdist-1], represented_units, coefs,
                                                        params, duration_avg_hours, duration_pop_weight, hh_storage,
                                                        month_factors, hh_tanker_prices, is_sewage,
                                                        sewage_pop_weight))
                    human_rf_agents[count].institution_names = ['human_agent_wrapper', gov_wrappers[gov-1],
                                                                ("subdist_" + str(subdist_code) + "_wrapper")]
                    human_rf_agents[count].is_camp_location = is_camp_location
                    is_camp_location = False
                    count += 1
    count = 0
    for subdist in range(1, 90):
        subdist_code = in_subdist_code[subdist-1]
        gov = in_subdist_gov[subdist-1]
        gov_name = in_gov_name[subdist-1]
        if in_subdist_has_industry[subdist-1] == 1:
            name = 'in_agent_' + str(subdist_code)
            human_in_agents.append(HumanINAgent(name, gov, gov_name, (subdist-1), subdist_code, str(in_subdist_name[subdist-1]),
                                                in_subdist_x[subdist-1], in_subdist_y[subdist-1],
                                                in_subdist_industry_name[subdist-1], in_subdist_is_active[subdist-1],
                                                in_subdist_piped_water_use[subdist-1], in_tariff,
                                                in_subdist_well_water_use[subdist-1], in_groundwater_cost,
                                                in_subdist_groundwater_node[subdist-1],
                                                in_subdist_surface_water_use[subdist-1], in_surface_water_cost,
                                                in_subdist_water_value_per_m3[subdist-1],
                                                in_subdist_water_source_names[subdist-1],
                                                in_month_factors))
            human_in_agents[count].institution_names = ['human_agent_wrapper', gov_wrappers[gov-1],
                                                        ("subdist_" + str(subdist_code) + "_wrapper")]
            count += 1
    log.info('Completed Urban Water User Agent Setup')


def get_gw_nodes():
    """Get list of groundwater nodes.

    Args:
        None

    Returns:
        gw_nodes (LIST): list of groundwater node objects

    """

    return gw_nodes


def get_network_nodes():
    """Get list of network nodes.

    Args:
        None

    Returns:
        network_nodes (LIST): list of network node objects

    """

    return network_nodes

def get_ww_nodes():
    """Get list of wastewater nodes.

    Args:
        None

    Returns:
        network_nodes (LIST): list of network node objects

    """

    return ww_nodes


def get_urb_nodes():
    """Get list of urban (household) nodes.

    Args:
        None

    Returns:
        urb_nodes (LIST): list of urban (household) node objects

    """

    return urb_nodes


def get_hfarm_nodes():
    """Get list of urban (household) nodes.

    Args:
        None

    Returns:
        urb_nodes (LIST): list of urban (household) node objects

    """

    return hfarm_nodes


def get_jvfarm_nodes():
    """Get list of jordan valley farmer nodes.

    Args:
        None

    Returns:
        jvfarm_nodes (LIST): list of jordan valley farmer node objects

    """

    return jvfarm_nodes


def get_network_links():
    """Get list of main network links.

    Args:
        None

    Returns:
        network_links (LIST): list of main network link objects

    """

    return network_links


def get_gw_network_links():
    """Get list of groundwater node-network node links.

    Args:
        None

    Returns:
        gw_network_links (LIST): list of groundwater node-network node link objects

    """

    return gw_network_links


def get_urb_network_links():
    """Get list of urban (household) node-network node links.

    Args:
        None

    Returns:
        urb_network_links (LIST): list of urban (household) node-network node link objects

    """

    return urb_network_links


def get_hfarm_gw_links():
    """Get list of highland farm node-groundwater node links.

    Args:
        None

    Returns:
        hfarm_gw_links (LIST): list of highland farm node-groundwater node link objects

    """

    return hfarm_gw_links


def get_jvfarm_network_links():
    """Get list of jordan valley farm node-network node links.

    Args:
        None

    Returns:
        urb_network_links (LIST): list of jordan valley farm node-network node link objects

    """

    return jvfarm_network_links

def get_human_hh_agents():
    """Get list of household agents. These are basic PyNSim components.

    Args:
        None

    Returns:
        human_hh_agents (LIST): list of household agent objects

    """

    return human_hh_agents

def get_human_co_agents():
    """Get list of commercial agents. These are basic PyNSim components.

    Args:
        None

    Returns:
        human_co_agents (LIST): list of commercial agent objects

    """

    return human_co_agents

def get_human_rf_agents():
    """Get list of refugee agents. These are basic PyNSim components.

    Args:
        None

    Returns:
        human_rf_agents (LIST): list of refugee agent objects

    """

    return human_rf_agents

def get_human_in_agents():
    """Get list of large industry agents. These are basic PyNSim components.

    Args:
        None

    Returns:
        human_in_agents (LIST): list of large industry agent objects

    """

    return human_in_agents
