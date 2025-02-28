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

from JWM.components.nodes.network_nodes import JordanNode
import logging
import numpy as np
import pandas as pd
import itertools

log = logging.getLogger('farmer_module')

substitute_gw_node = {'110501': '110901', '120103': '120401', '120104': '120101', '140103': '140101', '210301': '210801',
                      '210701': '210101', '210901': '210801', '220104': '220303', '230102': '230103', '240101': '210801',
                      '240102': '210801', '240103': '210801', '240201': '120102', '310201': '310202', '310302': '140203',
                      '310501': '310101', '310601': '310301', '320201': '320101', '330201': '340102'}

class HighlandFarmAgent(JordanNode):
    """The Highland Farm node class."""

    description = "A node representing highland farms"
    colour = "darkgreen"
    shape = 'p'
    size = 'small'

    mnth_period = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    _empty_month_dict = {'Jan': 0.0, 'Feb': 0.0, 'Mar': 0.0, 'Apr': 0.0, 'May': 0.0, 'Jun': 0.0,
                         'Jul': 0.0, 'Aug': 0.0, 'Sep': 0.0, 'Oct': 0.0, 'Nov': 0.0, 'Dec': 0.0}
    crops = ['Banana', 'Barley', 'Citrus', 'CucurbitsS', 'CucurbitsW', 'Dates', 'DryBeans', 'Fodder', 'Grapes',
             'LeafVegS', 'LeafVegW', 'Olive', 'OtherFld', 'OtherTrees', 'OtherVegS', 'OtherVegW', 'PotatoS',
             'PotatoW', 'StoneFruit', 'TomatoesS', 'TomatoesW', 'Wheat']

    _empty_crop_dict = {c: 0.0 for c in crops}

    tup = [(w, s) for w in ['Rainfed','Irrigated'] for s in ['summer','winter']]
    idx = pd.MultiIndex.from_tuples(tup, names=['Source', 'Season'])
    df_null = pd.DataFrame(data=0.0, index=idx, columns=crops)

    df_null_by_crop_source = pd.DataFrame([(i, j) for i, j in itertools.product(crops, ['Irrigated','Rainfed'])], columns = ['Crop','WaterSource'])
    df_null_by_crop_source['value'] = 0.0

    first_planning_run = False

    _properties = {
        # Properties with history used in calcs
        'tanker_offer_quantity_initial': 0.0,     # Level 0
        'final_tanker_price': 0.0,              # Level 0
        'gw_sale_to_tanker': 0,                 # Level 0
        'unit_pumping_cost': 0.0,             # Level 0

        # Properties with history need for primary outputs
        'tanker_offer_quantity': 0.0,           # Level 1
        'reservation_price': 0.0,               # Level 1
        'tanker_water_price': 0.0,              # Level 1
        'gw_supply': 0.0,                       # Level 1
        'gw_demand': 0.0,                       # Level 1
        'irrig_allocation': 0.0,                # Level 1
        'irrig_allocation_proj': 0.0,           # Level 1

        'gw_irrig_allocation': 0.0,  # Level 1
        'gw_irrig_allocation_proj': 0.0,  # Level 1

        'total_land_use': 0.0,                  # Level 1
        'cropping_profits': 0.0,                # Level 1
        'cropping_profits_proj': 0.0,           # Level 1
        'gw_sale_to_tanker_profits': 0.0,       # Level 1
        'total_farmer_profits_proj': 0.0,       # Level 1
        'total_farmer_profits': 0.0,            # Level 1
        'cropping': dict(_empty_crop_dict),     # Level 1
        'pumping_lift': None,       			# Level 1

        'simulated_irrigated_crop_prices': dict(_empty_crop_dict),
        'simulated_rainfed_crop_prices': dict(_empty_crop_dict),

        # Properties with history need for secondary outputs
        'reservation_price_fc_summer':  dict(_empty_month_dict),
        'reservation_price_fc_winter':  dict(_empty_month_dict),
        'reservation_price_fc_annual':  dict(_empty_month_dict),
        'tanker_supply_fc_summer':      dict(_empty_month_dict),
        'tanker_supply_fc_winter':      dict(_empty_month_dict),
        'tanker_supply_fc_annual':      dict(_empty_month_dict),
        'irrig_supply_fc_summer':       dict(_empty_month_dict),
        'irrig_supply_fc_winter':       dict(_empty_month_dict),
        'irrig_supply_fc_annual':       dict(_empty_month_dict),
        'irrig_supply_ac_summer':       dict(_empty_month_dict),
        'irrig_supply_ac_winter':       dict(_empty_month_dict),
        'irrig_supply_ac_annual':       dict(_empty_month_dict),

        'gw_irrig_supply_fc_summer': dict(_empty_month_dict),
        'gw_irrig_supply_fc_winter': dict(_empty_month_dict),
        'gw_irrig_supply_fc_annual': dict(_empty_month_dict),
        'gw_irrig_supply_ac_summer': dict(_empty_month_dict),
        'gw_irrig_supply_ac_winter': dict(_empty_month_dict),
        'gw_irrig_supply_ac_annual': dict(_empty_month_dict),

        'monthly_demand_forecast':      dict(_empty_month_dict),
        'monthly_price_forecast':       dict(_empty_month_dict),
        'init_tanker_demand':           dict(_empty_month_dict),
        'irrig_reduction':              dict(_empty_month_dict),
        'summer_demand_forecast': 0.0,
        'winter_demand_forecast': 0.0,
        'sales_history': [],
        '_hist': [],

        'nxscn': df_null.copy(deep=True),
        'gwscn': df_null.copy(deep=True),
        'gwscn_proj':df_null.copy(deep=True),
        'irrgscn': df_null.copy(deep=True),
        'yieldscn': df_null.copy(deep=True),
        'tanksupsc': df_null.copy(deep=True),

        'simulated_crop_prices': df_null_by_crop_source.copy(deep=True),
        'low_price_adj': df_null_by_crop_source.copy(deep=True),

        'tanker_gross_revenue': 0.0,
        'tanker_pumping_costs':  0.0,
        'tanker_abstraction_fees': 0.0,

        'HS': [],
        'HSmean': 0.0,
        'forecast_sales': 0.0,
        'expected_sales': 0.0,

        'pumping_capacity': 0.0,
        'well_reduction_pcnt': 0.0,
        'baseline_pumping': 0.0,

        'cir_delta_actl': dict(_empty_month_dict),
        'cir_delta_proj': dict(_empty_month_dict),

        'gw_supply_reduction_factor': 1.0,

        'tanker_well_capacity_initial': 0.0,
        'tanker_well_capacity': 0.0,
        'tanker_well_low_salinity_share': 0.0,
    }

    # Properties used in network_setup.py
    observed = {}
    land_inputs = None
    water_inputs = None
    land_costs = None
    water_costs = None
    crop_price = None
    crop_yield = None
    cropkc = None
    pcf = 0.0
    ref_pump_head = 0.0
    alphaR = None
    gammaR = None
    cwd_mnth = None
    irrg_prop = None
    yield_RF_fact = None
    rain_avg = dict(_empty_month_dict)
    rain_yr = None
    gw_abstraction_fee = 0.0

    crop_list = []
    input_types = []
    perennials = []

    tanker_supply = 0.0

    init_tanker_price = dict(_empty_month_dict)
    init_reservation_price = dict(_empty_month_dict)

    irrig_compensation = None

    # Other Properties definitely needed
    subdistrict = None
    is_active_in_tanker_market = True

    lift_start = 0.0
    lift_increase = None
    projected_demand_growth = 1.0
    projected_price_growth = 1.0

    crop_irrg_req_delta_actl = None
    crop_irrg_req_delta_proj = None

    cir_proj = None
    cir_actl = None
    cir_delta = dict(_empty_month_dict)


    gw_percentage = {}

    gw_demand_month_ms = 0.0

    irrig_demand = 0.0
    irrig_supply = 0.0

    # To be reviewd
    area = 0
    farm_class = 'generic'

    # Check in engine
    tanker_offer_key = 0
    tanker_market_revenue = 0
    crop_revenue_factor = 0
    res_price_1 = 0
    res_price_2 = 0
    ag_reduction_factor = 0
    ag_profit = 0
    tanker_profit_2 = 0
    tanker_profit = 0
    total_profit = 0
    volume_available = 0
    ag_allocation_tanker = None
    tanker_supply_offer = 0.0
    cir_adjust = dict((el, 0.0) for el in crops)
    gw_demand_annual = 0.0

    tanker_demand_forecast = 0.0
    tanker_price_forecast = 0.0

    irrig_alloc_surplus = 0.0
    tanker_offer = 0.0
    tanker_demand = 0.0
    tanker_price = 0.0
    #gw_demand = 0.0
    crop_supply_rainfed = 0.0
    crop_supply_irrig = 0.0
    total_rainfed = {}
    total_irrigated = {}
    ts = 0
    hist_range = []
    calibration_cost = 0.0
    gw_tax_tanker = 0.25



    def __init__(self, name, **kwargs):
        """ Initialise farm agent set corresponding instance of farm policy and archive class
        """
        super(HighlandFarmAgent, self).__init__(name, **kwargs)
        self.name = name
        self.still_has_tanker_water = True
        self.calibration_cost = 0.0

        self.baseline_well_reduction_pcnt = 1.0

    def setup(self, timestamp):
        pass

    #--- FARMER PLANNING ENGINE METHODS ---#
    # Methods to get groundwater status #
    def get_pumping_lift(self):
        """ Calculate lift increase (lift increase for simulated month - starting groundwater lift) """
        self.pumping_lift = 0
        for g in self.upstream_nodes:
            self.pumping_lift += g.lift * self.gw_percentage[g.name]

        return self.pumping_lift

    def get_wells_dry(self):
        # Check if wells are dry
        self.well_reduction_pcnt = 0.0
        self.baseline_pumping = 0.0
        self.pumping_capacity = 0.0

        for g in self.upstream_nodes:
            self.baseline_pumping += g._gw_node_properties['baseline_pumping']
            self.pumping_capacity += g.capacity_reduction_factor*g._gw_node_properties['baseline_pumping']
        if self.baseline_pumping > 0:
            self.well_reduction_pcnt = self.pumping_capacity / self.baseline_pumping
        else:
            self.well_reduction_pcnt = 1
        if len(self.upstream_nodes) == 0.0:
            self.well_reduction_pcnt = 1.0

        if self.network.current_timestep_idx == 0:
            self.baseline_well_reduction_pcnt = self.well_reduction_pcnt
        if self.baseline_well_reduction_pcnt > 0.0:
            self.well_reduction_pcnt = self.well_reduction_pcnt / self.baseline_well_reduction_pcnt
        else:
            self.well_reduction_pcnt = 0.0

    def set_tanker_offer_simple(self, supply_increase_factor):
        sim_month = self.network.current_timestep.strftime('%b')
        _multiplier = 1.0 + (supply_increase_factor - 1.0) * (self.network.current_timestep_idx / 12)
        self.tanker_offer_quantity_initial = float(self.init_tanker_demand[sim_month] * _multiplier)
        self.tanker_offer_quantity = self.tanker_offer_quantity_initial

    def set_tanker_offer_price(self):
        sim_month = self.network.current_timestep.strftime('%b')

        month_num = self.network.current_timestep.month
        if month_num in range(6,12):
            self.reservation_price = float(self.reservation_price_fc_summer[sim_month])
        else:
            self.reservation_price = float(self.reservation_price_fc_annual[sim_month])

        min_tanker_water_cost = max(0.0, self.pumping_lift) * self.pcf + self.network.parameters.farms['gw_tanker_tax']
        if self.reservation_price < min_tanker_water_cost:
            self.reservation_price = min_tanker_water_cost

        self.tanker_water_price = self.reservation_price

        self.tanker_water_price = self.reservation_price
        self.tanker_water_price *= self.network.parameters.farms['price_factor']  # For sensitivity testing

    def set_irrigation_allocation(self):
        sim_month = self.network.current_timestep.strftime('%b')
        self.irrig_allocation_proj = float(self.irrig_supply_fc_summer[sim_month]) + \
                                                float(self.irrig_supply_fc_winter[sim_month])
        self.irrig_allocation = float(self.irrig_supply_ac_summer[sim_month]) + \
                                                float(self.irrig_supply_ac_winter[sim_month])

        self.gw_irrig_allocation_proj = float(self.gw_irrig_supply_fc_summer[sim_month]) + \
                                        float(self.gw_irrig_supply_fc_winter[sim_month])
        self.gw_irrig_allocation = float(self.gw_irrig_supply_ac_summer[sim_month]) + \
                                   float(self.gw_irrig_supply_ac_winter[sim_month])

    def set_tanker_offer_endogenous(self, supply_increase_factor):
        _multiplier = 1.0 + (supply_increase_factor - 1.0) * (self.network.current_timestep_idx / 12)
        self.tanker_well_capacity = float(self.tanker_well_capacity_initial * _multiplier * self.well_reduction_pcnt)
        self.tanker_offer_quantity = max(0.0, ((self.tanker_well_capacity - self.gw_irrig_allocation) *
                                               self.tanker_well_low_salinity_share))
        self.tanker_offer_quantity *= self.network.parameters.farms['offer_volume_factor']  # For sensitivity testing

        ################################################
        # Tanker market policy 1: Well closure - START #
        ################################################
        if self.network.simulation_type == "tanker":
            if self.network.parameters.tanker['tanker_policy_selection'] == 1:
                adjustment_speed = 1.0
                haw = self.network.get_institution("human_agent_wrapper")
                ts_now1 = self.network.current_timestep_idx
                if ts_now1 <= 11:
                    self.network.parameters.well_cap_reduction = 0.
                    self.network.parameters.well_cap_adjustment = 0.05
                    self.network.parameters.well_cap_reduction_history = np.repeat(0., 12).tolist()
                    self.network.parameters.well_cap_deviation_history = np.repeat(0., 12).tolist()
                    self.network.parameters.well_cap_tanker_offer_history = np.repeat(0., 12).tolist()
                    self.network.parameters.last_update_ts = 11
                else:
                    if self.network.parameters.last_update_ts < ts_now1:
                        lower_rate = np.linspace(0., 0., 36 * 12)[ts_now1]
                        upper_rate = np.linspace(40.E6, 80.E6, 36 * 12)[ts_now1]
                        last_target = np.linspace(58560000., 58560000., 36 * 12)[ts_now1 - 1]
                        past_years = self.network.current_timestep.year - 2015
                        last_month = self.network.current_timestep.month - 2
                        past_last_months = [((i * 12) + last_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        sales_sum_past_years = sum(haw.get_history("total_tanker_consumption")[i] for i in past_all_months)
                        sales_sum_past_last_months = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_last_months)
                        sales_share_past_avg_last_month = sales_sum_past_last_months / sales_sum_past_years
                        last_month_target = last_target * sales_share_past_avg_last_month
                        last_month_sales = haw.get_history("total_tanker_consumption")[-1]
                        deviation_pct = (last_month_sales / last_month_target) - 1.0
                        well_cap_adjustment = self.network.parameters.well_cap_adjustment + (
                                    deviation_pct * adjustment_speed)
                        self.network.parameters.well_cap_adjustment = max(0.0, well_cap_adjustment)
                        self.network.parameters.well_cap_reduction = \
                            max(lower_rate,
                                (lower_rate + self.network.parameters.well_cap_adjustment * (upper_rate - lower_rate)))
                        self.network.parameters.well_cap_reduction_history += [self.network.parameters.well_cap_reduction]
                        self.network.parameters.well_cap_deviation_history += [deviation_pct]
                        print("Tanker water market analyses - well cap policy:")
                        print(" - Previous month sales: " + str(last_month_sales))
                        print(" - Well cap reduction: " + str(self.network.parameters.well_cap_reduction))
                        self.network.parameters.last_update_ts = ts_now1
                    past_years = self.network.current_timestep.year - 2015
                    this_month = self.network.current_timestep.month - 1
                    past_this_months = [((i * 12) + this_month) for i in range(past_years)]
                    past_all_months = [i for i in range(past_years * 12)]
                    my_sum_past_sales_this_month = sum(self.get_history("gw_sale_to_tanker")[i] * 365. / 12. for i in
                                                       past_this_months)
                    sum_past_sales_all_month = sum(haw.get_history("total_tanker_consumption")[i] for i in past_all_months)
                    my_past_sales_share_avg_this_month = my_sum_past_sales_this_month / sum_past_sales_all_month
                    my_tanker_reduction1 = self.network.parameters.well_cap_reduction * my_past_sales_share_avg_this_month
                    tanker_offer_quantity_reduced = self.tanker_offer_quantity - my_tanker_reduction1
                    self.tanker_offer_quantity = max(0.0, tanker_offer_quantity_reduced)
                    self.network.parameters.well_cap_tanker_offer_history += [self.tanker_offer_quantity]
        ##############################################
        # Tanker market policy 1: Well closure - END #
        ##############################################

    # --- CONSUMPTION ENGINE METHODS ---#
    # Irrigation supply methods #
    def det_irrig_demand(self):
        sim_month = self.network.current_timestep.strftime('%b')
        self.irrig_demand = self.irrig_allocation
        self.gw_irrig_demand = self.gw_irrig_allocation

    def det_irrig_supply(self):
        self.irrig_supply = max(self.irrig_allocation,0)
        self.gw_irrig_supply = max(self.gw_irrig_allocation, 0)

    def det_tanker_supply(self):
        sim_month = self.network.current_timestep.strftime('%b')
        if self.is_active_in_tanker_market:
            self.tanker_supply = min(self.tanker_offer_quantity, self.tanker_demand)
        else:
            self.tanker_supply = 0.0

    def recalc_gw_percentage(self):
        self.pumping_capacity = 0.0
        for g in self.upstream_nodes:
            self.pumping_capacity += g._gw_node_properties['baseline_pumping']*g.capacity_reduction_factor

        for g in self.upstream_nodes:
            if self.pumping_capacity > 0:
                self.gw_percentage[g.name] = g._gw_node_properties['baseline_pumping']*g.capacity_reduction_factor / self.pumping_capacity
            else:
                self.gw_percentage[g.name] = 0.0
        if len(self.upstream_nodes) == 0:
            pass

    def det_groundwater_abstraction(self):
        self.gw_demand = max(self.gw_irrig_supply, 0.0) + 30.41667 * self.gw_sale_to_tanker

        if self.subdistrict in substitute_gw_node.keys():
            self.gw_supply = 0
            substitute_name = str(substitute_gw_node[self.subdistrict]) + '_hfarm'
            self.network.get_node(substitute_name).gw_supply += self.gw_demand
        else:
            self.gw_supply += self.gw_demand
        return self.gw_supply

    def reset_groundwater_supply(self):
        self.gw_supply = 0

    def set_groundwater_pumping(self):
        """ Groundwater supplies at farm are aggregated to groundwater node """
        month_to_sec = 1.0 / (60.0 * 60.0 * 24.0 * 365 / 12.0)
        self.gw_demand_month_ms = self.gw_supply * month_to_sec
        for g in self.upstream_nodes:
            g.pumping = self.gw_demand_month_ms * self.gw_percentage[g.name]

    def det_farmer_profits(self):
        self.tanker_gross_revenue = self.final_tanker_price * self.gw_sale_to_tanker * 30.41667
        _lift = self.pumping_lift
        if _lift < 0.0:
            _lift = 0.0
        self.tanker_pumping_costs = self.gw_sale_to_tanker * _lift * self.pcf * 30.41667
        _tanker_tax = self.network.parameters.farms['gw_tanker_tax']
        self.tanker_abstraction_fees = self.gw_sale_to_tanker * 30.41667 * _tanker_tax

        self.gw_sale_to_tanker_profits = self.tanker_gross_revenue - self.tanker_pumping_costs - self.tanker_abstraction_fees


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++                                                                                                +++++
# +++                                        JORDAN VALLEY AGENT                                         +++
# +++++                                                                                                +++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JVFarmAgentSimple(JordanNode):
    """Simple implementation of Jordan Valley Farm agent at the DA level
    """

    colour = 'darkgreen'
    shape  = 'p'

    _properties = {
        # Physical properties (exogenous)
        'area': 0,                      # float         # Exog: physical area of farm (ha)  ** CULTIVATED AREA **
        'veg_area': 0,
        'ban_area': 0,
        'cit_area': 0,
        'tanker_water_price': 0,
        'tanker_offer_key': 0,
        'tanker_offer_quantity': 0,
        'gw_sale_to_tanker': 0,
        'final_tanker_price': 0.0,
        'tanker_market_revenue': 0,
    }

    def __init__(self, name, **kwargs):
        """ Initialise farm agent set corresponding instance of farm policy and archive class
        """
        super(JVFarmAgentSimple, self).__init__(name, **kwargs)
        self.name = name