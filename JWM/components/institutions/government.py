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

from pynsim import Institution
import pandas as pd
from pyomo.environ import *
from pyomo.opt import SolverFactory
from pyomo.opt import SolverStatus, TerminationCondition
import math
import pyutilib.common

import logging
from copy import deepcopy

import os
from JWM import basepath
from JWM import get_excel_data

import numpy as np

swat_inputs_xlsx = get_excel_data("swat_inputs.xlsx")


class JordanInstitution(Institution):
    description = "Common Methods for Jordan Institutions"

    def __init__(self, name, **kwargs):
        super(JordanInstitution, self).__init__(name, **kwargs)
        self.gw_levels = {}
        self.res_levels = {}

    def get_gw_levels(self):
        self.gw_levels = {}
        for n in self.nodes:  # gathers gw level information
            if n.component_type == 'Groundwater':
                self.gw_levels[n.name] = n.head

    def get_res_levels(self):
        self.res_levels = {}
        for n in self.nodes:  # gathers reservoir level information
            if n.component_type == 'Reservoir':
                self.res_levels[n.name] = n.level

class CWA(JordanInstitution):
    """The Central Water Authority class for Jordan

    Scenario-based inputs:
        population (dict)

    Intervention-based inputs:
        yarmouk_to_kac (dict/int) - yarmouk river to kac flow
        tiberias_to_kac (dict/int) - lake tiberias to kac flow
        kac_to_zai (dict/int) - kac to zai transfer volume
        yearly_max_gw (dict/int) - maximum annual groundwater pumping per governorate

    Agent inputs:

    """
    description = "Central Water Authority"

    _properties = {
        'population_estimate': {},  # dictionary of population estimates, indexed by subdistrict code
        'demand_estimate': {},
        'gw_pumping': {},  # pumping is a dictionary of pumping amounts, indexed by unit stress location (gw node)
        'gw_pumping_forecast': {},  # pumping forecasts (time series dict), indexed by unit stress location (gw node)
        'waj_forecast': {},  # allocation forecast to WAJ for next 12 months
        'jva_forecast': {},  # allocation forecast to JVA for next 12 months
        'yarmouk_to_kac_forecast': [],
        'tiberias_to_kac_forecast': [],
        'kac_to_zai_forecast': [],
        'kac_to_zai': 0,
        'yearly_max_gw': {},
        'transfers': {
            'tiberias': {'kac_start': 0},
            'kac_start': {'tiberias': 0},
            'wehdah_res': {'kac_start': 0},
            'kac_north': {'zai': 0},
        },
        'wehdah_release_forecast': [],
        'wehdah_kac_forecast': [],
        'wehdah_volume_forecast': [],
        'wehdah_volume': [],
        'wehdah_release': [],
        'allocation_actual': 0,
        'concession_actual': 0,
        'alpha_kac_actual': 0,
        'extra_to_israel': 0,
        'tiberias_kac_actual': 0,
        'kac_zai_actual': 0,
        'adasiya_syr_inflow': 0,
    }

    def set_allocation(self):

        demand_node = ['demand']
        storage_node = ['wehdah_res']
        all_nodes = ['demand','wehdah_res']
        wehdah_node = self.network.get_node('wehdah_res')

        model = AbstractModel()
        model.months = Set(initialize=range(1, 13))
        model.nodes = Set(initialize=all_nodes)
        model.demand_nodes = Set(initialize=demand_node)
        model.storage_nodes = Set(initialize=storage_node)

        # Define dynamic parameters
        year = self.network.current_timestep.year
        month = self.network.current_timestep.month

        inflow = {}
        evap = {}

        # Define remaining months
        if month == 12:
            rem_months = [12]
            for m in range(11):
                rem_months.append(m + 1)
        else:
            rem_months = range(month, 13)
            if rem_months[0] != 1:
                for m in range(rem_months[0]-1):
                    rem_months.append(m+1)

        len_months = len(rem_months)
        model.rem_months = Set(initialize=rem_months)

        # Define past months
        if month == 12:
            past_months = []
        else:
            if self.network.current_timestep.year == self.network.timesteps[0].year:
                past_months = []
            else:
                past_months = [12]
            for m in range(month-1):
                past_months.append(m+1)

        model.past_months = Set(initialize=past_months)

        if month == 12 or self.network.current_timestep_idx == 0:
            self.past_deficit = {}
        elif self.network.current_timestep_idx == 1:
            self.past_deficit = {}
            self.past_deficit[('demand',month-1)] = (self.network.exogenous_inputs._wehdah_release_target[month-1] - self.wehdah_release) / self.network.exogenous_inputs._wehdah_release_target[month-1]
        else:
            for m in past_months:
                if month > m:
                    self.past_deficit[('demand',m)] = (self.network.exogenous_inputs._wehdah_release_target[m] - self.get_history('wehdah_release')[m-month]) / self.network.exogenous_inputs._wehdah_release_target[m]
                else:
                    self.past_deficit[('demand',m)] = (self.network.exogenous_inputs._wehdah_release_target[m] - self.get_history('wehdah_release')[m-(month+12)]) / self.network.exogenous_inputs._wehdah_release_target[m]

        model.past_def = Param(model.demand_nodes, model.past_months, initialize=self.past_deficit)

        # Project inflows to Wehdah
        # Assumes perfect foresight for current month, average values for future months
        if month == 12:
            inflow[('wehdah_res',12)] = 0
            evap[('wehdah_res',12)] = wehdah_node.evap
            inflow[('wehdah_res', 12)] = self.network.get_node('wehdah_res').inflow
            for m in range(11):
                inflow[('wehdah_res',m+1)] = 0
                if self.network.parameters.sw['syria_lu'] == 'war':
                    inflow[('wehdah_res',m+1)] += self.network.observations.inflow_averages['wehdah_res_postwar'][m+1]
                else:
                    inflow[('wehdah_res', m + 1)] += self.network.observations.inflow_averages['wehdah_res_prewar'][m+1]
                evap[('wehdah_res',m+1)] = self.network.observations.evap_averages['wehdah_res'][m+1]

        else:
            for m in range(1,13):
                inflow[('wehdah_res',m)] = 0
                if m == month:
                    evap[('wehdah_res',m)] = wehdah_node.evap
                    inflow[('wehdah_res', m)] = self.network.get_node('wehdah_res').inflow
                else:
                    if self.network.parameters.sw['syria_lu'] == 'war':
                        inflow[('wehdah_res',m)] += self.network.observations.inflow_averages['wehdah_res_postwar'][m]
                    else:
                        inflow[('wehdah_res',m)] += self.network.observations.inflow_averages['wehdah_res_prewar'][m]
                    evap[('wehdah_res',m)] = self.network.observations.evap_averages['wehdah_res'][m]

        model.evap = Param(model.storage_nodes, model.rem_months, initialize=evap)
        model.inflow = Param(model.storage_nodes, model.rem_months, initialize=inflow)

        initial_storage = {}
        min_storage = {}
        avg_storage = {}

        if self.network.current_timestep_idx == 0:
            initial_storage['wehdah_res'] = wehdah_node.volume
        elif self.network.current_timestep_idx == 1:
            initial_storage['wehdah_res'] = wehdah_node.volume - wehdah_node.get_history('release')[-1] + 1145846.0
        else:
            initial_storage['wehdah_res'] = wehdah_node.get_history('volume')[-1]

        # Update minimum / average reservoir requirements
        if month == 12:
            for m in range(12):
                min_storage[('wehdah_res', m+1)] = 0

        else:
            for m in range(1,13):
                min_storage[('wehdah_res', m)] = 0

        model.storage_lower_bound = Param(model.storage_nodes, model.rem_months, initialize=min_storage)

        model.initial_storage = Param(model.storage_nodes, initialize=initial_storage)

        # Identify demand target. Based on 2013-2015 observed releases.
        current_demand_forecast = {}
        current_month = self.network.current_timestep.month
        if month == 12:
            for m in range(12):
                if m + month <= 12:
                    current_demand_forecast[('demand',m+1)] = self.network.exogenous_inputs._wehdah_release_target[m+month]
                else:
                    current_demand_forecast[('demand',m+1)] = self.network.exogenous_inputs._wehdah_release_target[m+month-12]
        else:
            for m in range(1,13):
                current_demand_forecast[('demand',m)] = self.network.exogenous_inputs._wehdah_release_target[m]

        model.demand = Param(model.demand_nodes, model.rem_months, initialize=current_demand_forecast)

        # Define Decision Variables
        model.release = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)  # Reservoir releases
        model.storage = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)  # Reservoir storage
        model.spill = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)  # Reservoir spill

        def objective_function(model):
            """
            Objective function for JVA Pyomo optimization: Minimizes deficit between target delivery and actual deliver
            """
            return sum(model.demand[d_node, month] - model.release[s_node,month] for d_node in model.demand_nodes for s_node in model.storage_nodes for month in model.rem_months)

        model.Z = Objective(rule=objective_function, sense=minimize)

        def res_balance_storage(model, node, month):

            if month == self.network.current_timestep.month:
                return model.storage[node, month] == model.initial_storage[node]
            else:
                if month == 1:
                    return model.storage[node, month] - model.storage[node, 12] + model.release[node, 12] - model.inflow[node, 12] \
                           + model.evap[node, 12] + model.spill[node,12] == 0
                else:
                    return model.storage[node, month] - model.storage[node, month-1] + model.release[node, month-1] - model.inflow[node, month-1] \
                          + model.evap[node, month-1] + model.spill[node,month-1] == 0

        model.res_balance_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_balance_storage)

        def smooth_deficit_month_lesser_v2(model, node, month, other_month):
            """
            Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months within 10 percent
            """

            if other_month == month:
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.release['wehdah_res', month]) / model.demand[node, month] <= 1.1 * (model.demand[node, other_month] - model.release['wehdah_res', other_month]) / model.demand[node, other_month]

        model.smooth_deficit_month_lesser_v2 = Constraint(model.demand_nodes, model.rem_months, model.rem_months, rule=smooth_deficit_month_lesser_v2)

        def smooth_deficit_month_greater_v2(model, node, month, other_month):
            """
            Greater than constraint to smooth out deficits over time. Normalized deficit between successive months within 10 percent
            """

            if other_month == month:
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.release['wehdah_res', month]) / model.demand[node, month] >= 0.9 * (model.demand[node, other_month] - model.release['wehdah_res', other_month]) / model.demand[node, other_month]

        model.smooth_deficit_month_greater_v2 = Constraint(model.demand_nodes, model.rem_months, model.rem_months, rule=smooth_deficit_month_greater_v2)

        def smooth_deficit_past_month_greater_v2(model, node, month, past_month):
            """
            Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months within 10 percent
            """

            if current_month == 12 or self.network.current_timestep_idx == 0:
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.release['wehdah_res', month]) / model.demand[node, month] >= 0.9 * model.past_def[node,past_month]

        model.smooth_deficit_past_month_greater_v2 = Constraint(model.demand_nodes, model.rem_months, model.past_months, rule=smooth_deficit_past_month_greater_v2)

        def res_minimum_storage(model, node, month):
            """
            Constraint for min storage at a reservoir. Based on historically observed min storage
            """
            return model.storage[node, month] >= 0

        model.res_minimum_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_minimum_storage)

        def res_maximum_storage(model, node, month):
            """
            Constraint for max storage at a reservoir. Based on reservoir storage capacity
            """
            return model.storage[node, month] <= wehdah_node.res_properties['live_storage_capacity']

        model.res_maximum_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_maximum_storage)

        def delivery_capacity(model, node, month):
            """
            Constraint to prevent delivery from exceeding target demand
            """
            return model.release['wehdah_res', month] <= model.demand[node, month]

        model.delivery_capacity = Constraint(model.demand_nodes, model.rem_months, rule=delivery_capacity)

        opt = SolverFactory("ipopt", solver_io='nl')
        instance = model.create_instance()
        instance.dual = Suffix(direction=Suffix.IMPORT)
        try:
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "ipopt"
            solver_status = "ok"
        except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
            solver_status = "ipopt_failed"
        if solver_status != "ipopt_failed":
            if (results.solver.status == SolverStatus.ok) \
                    and (results.solver.termination_condition == TerminationCondition.optimal):
                pass
            else:
                logging.info(" ... retrying")
                opt = SolverFactory("cplex")
                instance = model.create_instance()
                instance.dual = Suffix(direction=Suffix.IMPORT)
                results = opt.solve(instance)
                model.solutions.load_from(results)
                # logging.info(results)
                solver_name = "cplex"
        else:
            logging.info(" ... retrying")
            opt = SolverFactory("cplex")
            instance = model.create_instance()
            instance.dual = Suffix(direction=Suffix.IMPORT)
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "cplex"
        if (results.solver.status == SolverStatus.ok) and (
            results.solver.termination_condition == TerminationCondition.optimal):
            logging.info("cwa solved using " + solver_name)
        else:
            logging.info("cwa failed to solve")

        for var in model.component_objects(Var, active=True):
            var = str(var)
            if var == "storage":
                p_var = getattr(instance, var)
                total=0
                for vv in p_var:
                    name= ''.join(map(str,vv))
                    total=total+p_var[vv].value
                    # Set reservoir node storage value to optimization result (for intial timestep, equals storage at
                    #  period 1, otherwise equals storage at period 2)
                    if self.network.current_timestep_idx == 0:
                        if vv[1] == self.network.current_timestep.month:
                            self.get_node(str(vv[0])).volume = p_var[vv].value
                    else:
                        if self.network.current_timestep.month == 12:
                            if vv[1] == 1:
                                self.get_node(str(vv[0])).volume = p_var[vv].value
                        else:
                            if vv[1] == self.network.current_timestep.month + 1:
                                self.get_node(str(vv[0])).volume = p_var[vv].value
            elif var == "release":
                self.wehdah_release_forecast = [0] * len_months
                month = self.network.current_timestep.month
                d_var = getattr(instance, var)
                total=0
                for vv in d_var:
                    name= ''.join(map(str,vv))
                    total = total+d_var[vv].value
                    # Set reservoir node release value to optimization result (for month 1 only)
                    if vv[1] == month:
                        self.network.get_node(str(vv[0])).release = d_var[vv].value
                        self.wehdah_release = d_var[vv].value
                        self.wehdah_release_forecast[0] = d_var[vv].value
                    else:
                        if vv[1] - month > 0:
                            self.wehdah_release_forecast[vv[1] - month] = d_var[vv].value
                        else:
                            # print month
                            # print vv[1]
                            self.wehdah_release_forecast[(12-month)+vv[1]] = d_var[vv].value

    def det_wehdah_to_kac_forecast(self):
        self.wehdah_kac_forecast = []
        for m in range(12):
            self.wehdah_kac_forecast.append(self.wehdah_release_forecast[m] * .91)

    def det_kac_to_zai_forecast(self):
        """Determine KAC to Zai transfer forecast.

        Currently use CWA as an exogenous interface for determining KAC to Zai transfer

        **Intervention-based inputs**:

            |  *kac_to_zai* (dict) - yarmouk river to kac flow

        **Agent inputs**:

            |  None

        **Hydro inputs**:
            |  None

        """

        self.kac_to_zai_forecast = []
        current_month = self.network.current_timestep.month
        current_year = self.network.current_timestep.year

        # Switch to using average of observed flows for forecast (to avoid perfect foresight)
        inputs_swat = swat_inputs_xlsx.parse('swat_inputs')
        syria_lu_string = self.network.parameters.sw['rcp'] + ' ' + self.network.parameters.sw['syria_lu']
        inputs_adasiya = inputs_swat[(inputs_swat.index1_value == 'adasiya') & (inputs_swat.scenario == syria_lu_string)]

        if self.network.parameters.cwa['cwa_dynamic'] == 1:
            self.kac_to_zai_forecast = []
            for m in range(12):
                if m == 0:
                    total_kac_supply = (self.wehdah_kac_forecast[m]) + inputs_adasiya[(inputs_adasiya.index2_value == current_year) & (inputs_adasiya.index3_value == current_month + m)].value.values[0] + \
                        self.network.observations.cwa_transfer_averages['tiberias_kac_n'][current_month + m] + \
                        self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][current_month + m]
                else:
                    if current_month + m <= 12:
                        total_kac_supply = (self.wehdah_kac_forecast[m]) + self.network.observations.cwa_transfer_averages['yarmouk_kac_n'][current_month + m] + \
                            self.network.observations.cwa_transfer_averages['tiberias_kac_n'][current_month + m] + \
                            self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][current_month + m]
                    else:
                        total_kac_supply = (self.wehdah_kac_forecast[m]) + self.network.observations.cwa_transfer_averages['yarmouk_kac_n'][current_month + m - 12] + \
                            self.network.observations.cwa_transfer_averages['tiberias_kac_n'][current_month + m - 12] + \
                            self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][current_month + m - 12]
                kac_to_zai_forecast = ((.2685 * total_kac_supply) + 2e+06) * self.network.parameters.cwa['cwa_waj_factor']
                self.kac_to_zai_forecast.append(kac_to_zai_forecast)


        # Determine actual (current month) deliveries to kac and transfers with Israel based on agreement
        if self.network.parameters.cwa['cwa_dynamic'] == 1:
            allocation_target = [1.91, 1.73, 1.91, 1.85, 2.19, 2.34, 2.42, 2.42, 2.34, 2.15, 1.85, 1.91]
            allocation_target = [element * 1000000 for element in allocation_target]
            concession_target = [2.94, 2.66, 2.94, 2.84, 1.40, 0, 0, 0, 0, 1.44, 2.84, 2.94]
            concession_target = [element * 1000000 for element in concession_target]
            tiberias_target = [0.17, 0.34, 2.27, 4.30, 5.89, 5.96, 6.01, 6.16, 5.88, 5.51, 4.14, 2.00]
            tiberias_target = [element * 1000000 for element in tiberias_target]
            kac2008_target = [6.2, 5.0, 7.0, 7.6, 9.0, 10.3, 10.9, 11.4, 10.4, 9.2, 7.5, 5.6]
            kac2008_target = [element * 1000000 for element in kac2008_target]

            self.adasiya_syr_inflow = inputs_adasiya[(inputs_adasiya.index2_value == current_year) & (inputs_adasiya.index3_value == current_month)].value.values[0]
            adasiya_inflow = self.wehdah_release_forecast[0] + self.adasiya_syr_inflow

            # First, try to meet the allocation and concession targets to Israel
            if adasiya_inflow <= allocation_target[current_month-1] + concession_target[current_month-1]:
                if adasiya_inflow <= allocation_target[current_month-1]:
                    self.allocation_actual = allocation_target[current_month-1]
                    self.concession_actual = 0
                else:
                    self.allocation_actual = allocation_target[current_month-1]
                    self.concession_actual = adasiya_inflow - allocation_target[current_month-1]
            else:
                self.allocation_actual = allocation_target[current_month-1]
                self.concession_actual = concession_target[current_month-1]

            if self.network.current_timestep_idx <= 11:
                if self.network.current_timestep_idx == 0:
                    tiberias_kac_adjustment = 0
                    self.tiberias_kac_actual = max(0, tiberias_target[current_month - 1] + tiberias_kac_adjustment)
                else:
                    no_prev_month = len(self.get_history('concession_actual'))
                    tiberias_kac_adjustment = (np.sum(self.get_history('concession_actual')) + np.sum(concession_target[no_prev_month:]) - np.sum(
                        concession_target)) / 12.0
                    self.tiberias_kac_actual = max(0, tiberias_target[current_month - 1] + tiberias_kac_adjustment)
            else:
                tiberias_kac_adjustment = (np.sum(self.get_history('concession_actual')[-12:]) - np.sum(concession_target))/12.0
                self.tiberias_kac_actual = max(0, tiberias_target[current_month-1] + tiberias_kac_adjustment)

            if adasiya_inflow - self.allocation_actual - self.concession_actual > 0:
                adasiya_remaining = adasiya_inflow - self.allocation_actual - self.concession_actual
                if adasiya_remaining >= self.network.observations.cwa_transfer_2015['wehdah_kac_n'][current_month] + self.network.observations.cwa_transfer_2015['yarmouk_kac_n'][current_month]:
                    self.alpha_kac_actual = self.network.observations.cwa_transfer_2015['wehdah_kac_n'][current_month] + self.network.observations.cwa_transfer_2015['yarmouk_kac_n'][current_month]
                    self.extra_to_israel = adasiya_remaining - (self.network.observations.cwa_transfer_2015['wehdah_kac_n'][current_month] + self.network.observations.cwa_transfer_2015['yarmouk_kac_n'][current_month])
                else:
                    self.alpha_kac_actual = adasiya_remaining

            self.kac_zai_actual = (.2685 * (self.tiberias_kac_actual + self.alpha_kac_actual) + 2e+06)

    def determine_transfers(self):  # determines operational transfers for current timestep
        self.kac_to_zai = self.kac_to_zai_forecast[0]

class JVA(JordanInstitution):
    """The Jordan Valley Authority class.

    The Jordan Valley Authority (JVA) is the institution in charge of water management, allocation, and delivery to farmers in
    the Jordan Valley. The main decisions of the Jordan Valley in the model are to determine water allocations for Jordan
    Valley farmers, which the farmers use in turn to make cropping decisions, and actual monthly water deliveries to farmers.

    **Properties**:

        |  *wua_demand_estimate* (dict/int) - dictionary of farmer demand estimates, indexed by wua name
        |  *crop_quota* (dict/dict/int) - dictionary of monthly crop quotas, indexed by crop name
        |  *gw_pumping* (dict/int) - dictionary of pumping amounts, indexed by groundwater node name
        |  *gw_pumping_forecast* (dict/list/int) - dictionary of pumping forecasts, indexed by groundwater node name
        |  *demand_request* (int) - consolidation of all demand requests to submit to CWA
        |  *transfers* (dict/dict/int) - dictionary of transfer volumes, indexed by from and to node names
        |  *wua_delivery* (dict/int) - dictionary of deliveries to wua's, indexed by wua name

    **Scenario-based inputs**:

        |  *population* (dict/int) - Population
        |  *crop_prices* (dict/int) - Crop prices

    **Intervention-based inputs**:

        |  *base_crop_quota* (dict) - Baseline crop water quotas

    **Agent inputs**:
        |  *CWA - jva_forecast* (list/int): Forecast of water availability to the JVA
        |  *CWA - jva_delivery* (int): Delivery to JVA

    **Hydro inputs**:
        |  *GW - gw_levels* (int): Groundwater levels for the institution's groundwater nodes.
        |  *SW - res_levels* (int): Reservoir levels for the institution's reservoir nodes.

    """
    description = "Jordan Valley Authority"

    def __init__(self, name, **kwargs):
        super(JVA, self).__init__(name, **kwargs)
        self.model = None
        self.crop_groups = ['veg', 'cit', 'ban', 'oth', 'dat']
        self.node_delivery_history = []
        self.node_allocations_history = []

    _properties = {
        'demand_forecast_per_crop': {},  # demand forecast per node/crop/month (based on baseline crop water quota)
        'demand_forecast_per_node': {},  # forecast per node/crop/month (based on baseline crop water quota)
        'crop_quota': {},  # dictionary of monthly crop water quotas, indexed by crop name
        'gw_pumping': {},  # pumping is a dictionary of pumping amounts, indexed by unit stress location (gw node) names
        'gw_pumping_forecast': {},  # pumping forecasts (time series list), #indexed by unit stress location (gw node)
        'node_allocations': {},  # JV demand node allocations, indexed by node, crop & month (relative to current month)
        'mujib_annual_availability': 0, # Annual (next 12 months) of projected supply availability from Mujib dam
        'walah_annual_availability': 0, # Annual (next 12 months) of projected supply availability from Walah dam
        'node_delivery': {},


    }

    def setup(self, timestep):
        # Reset demand forecasts
        self.demand_forecast_per_crop = {}
        self.demand_forecast_per_node = {}

    # Added for Pyomo optimization
    def det_demand_forecast(self):
        """
        Determines demand forecast for each JV demand node and each crop group.

        The demand forecast is used to determine the target delivery, which is used in the JVA agent's optimization formulation.
        Demand is calculated based upon crop acreages and target monthly delivers for various crops.

        **Intervention-based inputs**:

            |  *base_crop_quota* (dict) - Baseline crop water quotas

        **Agent inputs**:

            |  *None*

        **Hydro inputs**:

            |  *None*

        **Outputs**:

            |  *self.demand_forecast_per_crop* (dict) - Demand forecast per demand node, per crop, per month
            |  *self.demand_forecast_per_crop* (dict) - Demand forecast per demand node, per month (i.e. summed over crops)
        """

        self.demand_forecast_per_crop = {}
        self.demand_forecast_per_node = {}

        for so in range(1,13):
            so_name = 'so' + str(so)
            for node in self.nodes:
                if node.component_type == 'JVFarmAgentSimple':
                    if node.so_mwi == so_name:
                        for crop in self.crop_groups:
                            if crop == 'oth':
                                if (node.so_mwi, crop, 1) not in self.demand_forecast_per_crop.keys():
                                    for future_month in range(12):
                                        self.demand_forecast_per_crop[(node.so_mwi, crop, future_month+1)] = 0
                            else:
                                if (node.so_mwi, crop, 1) in self.demand_forecast_per_crop.keys():
                                    for month in range(12):
                                        self.demand_forecast_per_crop[(node.so_mwi, crop, month+1)] += \
                                            self.network.exogenous_inputs._cwr[node.jv_region][crop][month+1] * \
                                                node.crop_areas_2015[crop] * 30.42  # -> daily to monthly water demand
                                else:
                                    for month in range(12):
                                        self.demand_forecast_per_crop[(node.so_mwi, crop, month+1)] = \
                                            self.network.exogenous_inputs._cwr[node.jv_region][crop][month+1] * \
                                                node.crop_areas_2015[crop] * 30.42  # -> daily to monthly water demand

            for future_month in range(12):
                self.demand_forecast_per_node[(so_name, future_month+1)] = 0
                for crop in self.crop_groups:
                    if (so_name, crop, 1) in self.demand_forecast_per_crop.keys():
                        self.demand_forecast_per_node[(so_name, future_month+1)] += \
                            self.demand_forecast_per_crop[(so_name, crop, future_month+1)]

        for month in range(12):
            demand_north = 0
            demand_middle = 0
            demand_south = 0
            for so in range(1,11):
                so_name = 'so' + str(so)
                if so_name == 'so1' or so_name == 'so2' or so_name == 'so7':
                    demand_north += self.demand_forecast_per_node[(so_name, month+1)]
                if so_name == 'so3' or so_name == 'so4' or so_name == 'so5' or so_name == 'so8':
                    demand_middle += self.demand_forecast_per_node[(so_name, month+1)]
                if so_name == 'so6' or so_name == 'so9' or so_name == 's10':
                    demand_south += self.demand_forecast_per_node[(so_name, month+1)]
            for so in range(1,11):
                so_name = 'so' + str(so)
                if so_name == 'so1' or so_name == 'so2' or so_name == 'so7':
                    self.demand_forecast_per_node[(so_name, month + 1)] = (self.demand_forecast_per_node[(so_name, month+1)] / demand_north) * self.network.observations.jva_sold_2015['north'][month+1] * 2
                if so_name == 'so3' or so_name == 'so4' or so_name == 'so5' or so_name == 'so8':
                    self.demand_forecast_per_node[(so_name, month + 1)] = (self.demand_forecast_per_node[(so_name, month+1)] / demand_middle) * self.network.observations.jva_sold_2015['middle'][month+1] * 2
                if so_name == 'so6' or so_name == 'so9' or so_name == 's10':
                    self.demand_forecast_per_node[(so_name, month + 1)] = (self.demand_forecast_per_node[(so_name, month+1)] / demand_south) * self.network.observations.jva_sold_2015['karameh'][month+1] * 2

    #  Added for Pyomo optimization
    def set_allocation_forecast(self):
        """
        Determines water allocation forecast to each JV zone and crop group for the next 12 months.

        Uses pyomo linear optimization to minimize deficit (relative to demand forecast calculated in det_demand_forecast
        method) at each demand node in JVA's system. The method sets up and solves the Pyomo optimization problem.

        **Intervention-based inputs**:

            |  *base_crop_quota* (dict) - Baseline crop water quotas

        **Same Agent Inputs**

            |  *demand_forecast_per_node* (dict) - Demand forecasts per demand node, per month

        **Other Agent Inputs**:

            |  *None*

        **Hydro inputs**:

            |  *None*

        **Outputs**:
        |  *allocation_forecast*
        """

        logging.info("Setting JVA farmer allocation forecast.")

        # Create Pyomo model object
        model = deepcopy(self.get_allocation_model())

        # Define dynamic parameters
        year = self.network.current_timestep.year
        if year == 2016:
            year = 2015
        month = self.network.current_timestep.month

        inflow = {}
        evap = {}
        seepage = {}
        exog_outflow = {}

        # Define remaining months
        if month == 12:
            rem_months = [12]
            for m in range(11):
                rem_months.append(m+1)
        else:
            rem_months = range(month,12)

        len_months = len(rem_months)

        model.rem_months = Set(initialize=rem_months)

        # Define past months
        if month == 12:
            past_months = []
        else:
            if self.network.current_timestep.year == self.network.timesteps[0].year:
                past_months = []
            else:
                past_months = [12]
            for m in range(month-1):
                past_months.append(m+1)

        model.past_months = Set(initialize=past_months)

        past_deficit = {}
        start_year = self.network.timesteps[0].year
        current_year = self.network.current_timestep.year
        if month != 12 and self.network.current_timestep_idx != 0:
            if start_year == current_year:
                for m in past_months:
                    if m == 12:
                        pass
                    else:
                        for n in self.nodes:
                            if n.component_type == 'AgDemand':
                                past_deficit[(n.name,m)] = ((self.demand_forecast_per_node[(n.name,m)] -
                                                             self.node_allocations_history[-(month-m)][(n.name,m)]) /
                                                            self.demand_forecast_per_node[(n.name,m)]) \
                                    if (self.demand_forecast_per_node[(n.name,m)] > 0.0) else 0.0
            else:
                for m in past_months:
                    if m == 12:
                        for n in self.nodes:
                            if n.component_type == 'AgDemand':
                                past_deficit[(n.name,12)] = ((self.demand_forecast_per_node[(n.name,12)] -
                                                              self.node_allocations_history[-month][(n.name,m)]) /
                                                             self.demand_forecast_per_node[(n.name,12)]) \
                                    if (self.demand_forecast_per_node[(n.name,12)] > 0.0) else 0.0
                    else:
                        for n in self.nodes:
                            if n.component_type == 'AgDemand':
                                past_deficit[(n.name,m)] = ((self.demand_forecast_per_node[(n.name,m)] -
                                                             self.node_allocations_history[-(month-m)][(n.name,m)]) /
                                                            self.demand_forecast_per_node[(n.name,m)]) \
                                    if (self.demand_forecast_per_node[(n.name,m)] > 0.0) else 0.0

        model.past_def = Param(model.demand_nodes, model.past_months, initialize=past_deficit)

        # Determine inflow to KAC
        inputs_swat = swat_inputs_xlsx.parse('swat_inputs')
        syria_lu_string = self.network.parameters.sw['rcp'] + ' ' + self.network.parameters.sw['syria_lu']
        inputs_adasiya = inputs_swat[(inputs_swat.index1_value == 'adasiya') & (inputs_swat.scenario == syria_lu_string)]

        # Assume perfect foresight for current month, average values for future months
        if month == 12:
            inflow[('kac_1',12)] = 0
            inflow[('kac_1',12)] += self.network.get_institution('cwa').alpha_kac_actual
            inflow[('kac_1', 12)] += self.network.get_institution('cwa').tiberias_kac_actual
            if math.isnan(self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][12]):
                logging.info('no data mukheiba_kac_n ' + str(year) + ' ' + str(12))
            else:
                inflow[('kac_1',12)] += self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][12]
            for m in range(11):
                inflow[('kac_1',m+1)] = 0
                inflow[('kac_1',m+1)] += self.network.observations.cwa_transfer_averages['yarmouk_kac_n'][m+1]
                inflow[('kac_1',m+1)] += self.network.observations.cwa_transfer_averages['wehdah_kac_n'][m+1]
                inflow[('kac_1',m+1)] += self.network.observations.cwa_transfer_averages['tiberias_kac_n'][m+1]
                inflow[('kac_1',m+1)] += self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][m+1]
                if self.network.parameters.cwa['cwa_dynamic'] == 1:
                    inflow[('kac_1', m + 1)] -= self.network.observations.cwa_transfer_averages['wehdah_kac_n'][m + 1]
                    inflow[('kac_1', m + 1)] += self.network.get_institution('cwa').wehdah_kac_forecast[m+1]
            for m in range(12):
                inflow[('kac_1', m + 1)] *= self.network.parameters.cwa['cwa_jva_factor']

        else:
            for m in range(12-month):
                inflow[('kac_1', m + month)] = 0
                if m == 0:
                    inflow[('kac_1', m + month)] += self.network.get_institution('cwa').alpha_kac_actual
                    inflow[('kac_1', m + month)] += self.network.get_institution('cwa').tiberias_kac_actual
                    if math.isnan(self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][month+m]):
                        logging.info('no data mukheiba_kac_n ' + str(year) + ' ' + str(month+m))
                    else:
                        inflow[('kac_1',m+month)] += self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][month+m]
                else:
                    inflow[('kac_1', month+m)] += self.network.observations.cwa_transfer_averages['yarmouk_kac_n'][month+m]
                    if self.network.parameters.cwa['cwa_dynamic'] == 1:
                        inflow[('kac_1', month + m)] += self.network.get_institution('cwa').wehdah_kac_forecast[m]
                    else:
                        inflow[('kac_1', month+m)] += self.network.observations.cwa_transfer_averages['wehdah_kac_n'][month+m]
                    inflow[('kac_1', month+m)] += self.network.observations.cwa_transfer_averages['tiberias_kac_n'][month+m]
                    inflow[('kac_1', month+m)] += self.network.observations.cwa_transfer_averages['mukheiba_kac_n'][month+m]
                inflow['kac_1',month+m] *= self.network.parameters.cwa['cwa_jva_factor']


        # Determine reservoir inflows (assumes perfect insight for current month, averages for future months)
        for res in self.nodes:
            if res.component_type == 'Reservoir':
                if month == 12:
                    inflow[(res.name, 12)] = res.inflow
                    evap[(res.name,12)] = res.evap
                    seepage[(res.name,12)] = res.res_properties['avg_seep']
                    for m in range(11):
                        inflow[(res.name,m+1)] = self.network.observations.inflow_averages[res.name][m+1]
                        evap[(res.name, m + 1)] = self.network.observations.evap_averages[res.name][m+1]
                        seepage[(res.name, m + 1)] = res.res_properties['avg_seep']
                        if res.name == 'kafrein_res':
                            seepage[(res.name, m + 1)] = .24 * res.volume
                else:
                    for m in range(12-month):
                        if m == 0:
                            inflow[(res.name,m+month)] = res.inflow
                            evap[(res.name,m+month)] = 0
                        else:
                            inflow[(res.name,m+month)] = self.network.observations.inflow_averages[res.name][m+month]
                            evap[(res.name,m+month)] = self.network.observations.evap_averages[res.name][m+month]
                        seepage[(res.name,m+month)] = res.res_properties['avg_seep']
                        if res.name == 'kafrein_res':
                            seepage[(res.name, m + month)] = .24 * res.volume
                    logging.info(str(res.name) + " inflow is " + str(inflow[(res.name,month)]))

        model.evap = Param(model.storage_nodes, model.rem_months, initialize=evap)
        model.seepage = Param(model.storage_nodes, model.rem_months, initialize=seepage)

        # Set other inflows equal to zero
        for i in self.nodes:
            if i.component_type != 'Reservoir' and i.component_type != 'JVFarmAgentSimple' and i.name != 'kac_1':
                if month == 12:
                    for m in range(12):
                        inflow[(i.name,m+1)] = 0
                else:
                    for m in range(12-month):
                        inflow[(i.name,m+month)] = 0

        model.inflow = Param(model.nodes, model.rem_months, initialize=inflow)

        # Other outflows, assumes perfect foresight of KAC-Amman transfers
        for n in self.nodes:
            if n.component_type != 'JVFarmAgentSimple':
                if month == 12:
                    if n.name == 'kac_7':
                        if self.network.parameters.cwa['cwa_dynamic'] == 1:
                            exog_outflow[(n.name, 12)] = self.network.get_institution('cwa').kac_zai_actual
                            for m in range(11):
                                exog_outflow[(n.name,m+1)] = self.network.get_institution('cwa').kac_to_zai_forecast[m+1]
                        else:
                            if math.isnan(self.network.observations.cwa_transfer_averages['kac_n_amman'][12]):
                                exog_outflow[(n.name,12)] = 1666666
                            else:
                                exog_outflow[(n.name,12)] = self.network.observations.cwa_transfer_averages['kac_n_amman'][12]
                            for m in range(11):
                                if math.isnan(self.network.observations.cwa_transfer_averages['kac_n_amman'][m+1]):
                                    exog_outflow[(n.name,m+1)] = 1666666
                                else:
                                    exog_outflow[(n.name,m+1)] = self.network.observations.cwa_transfer_averages['kac_n_amman'][m+1]
                    else:
                        for m in range(12):
                            exog_outflow[(n.name,m+1)] = 0
                else:
                    if n.name == 'kac_7':
                        if self.network.parameters.cwa['cwa_dynamic'] == 1:
                            for m in range(12-month):
                                exog_outflow[(n.name, m + month)] = self.network.get_institution('cwa').kac_to_zai_forecast[m]
                                exog_outflow[(n.name, month)] = self.network.get_institution('cwa').kac_zai_actual
                        else:
                            for m in range(12-month):
                                if math.isnan(self.network.observations.cwa_transfer_averages['kac_n_amman'][m+month]):
                                    exog_outflow[(n.name,m+month)] = 1666666
                                else:
                                    exog_outflow[(n.name,m+month)] = self.network.observations.cwa_transfer_averages['kac_n_amman'][year][m+month]
                    else:
                        for m in range(12-month):
                            exog_outflow[(n.name,m+month)] = 0

        model.exog_outflow = Param(model.nodes, model.rem_months, initialize=exog_outflow)

        # Update initial reservoir and min storage volumes
        initial_storage = {}
        min_storage = {}
        avg_storage = {}
        for res in self.nodes:
            if res.component_type == 'Reservoir':
                if self.network.current_timestep_idx == 0:
                    initial_storage[res.name] = res.volume
                elif self.network.current_timestep_idx == 1:
                    initial_storage[res.name] = res.volume - res.get_history('release')[-1] + self.network.observations.res_inflow_initial[res.name]
                else:
                    initial_storage[res.name] = res.get_history('volume')[-1]
                logging.info(res.name + " initial volume is " + str(initial_storage[res.name]))
                logging.info(res.name + " minimum storage is " + str(self.network.exogenous_inputs._min_res_storage[res.name][month]))

                # Update minimum / average reservoir requirements
                if month == 12:
                    for m in range(12):
                        min_storage[(res.name, m+1)] = self.network.exogenous_inputs._min_res_storage[res.name][m+1]
                        avg_storage[(res.name, m+1)] = self.network.observations.volume_averages[res.name][m+1]
                else:
                    for m in range(12-month):
                        min_storage[(res.name, m+month)] = self.network.exogenous_inputs._min_res_storage[res.name][m+month]
                        avg_storage[(res.name, m+month)] = self.network.observations.volume_averages[res.name][m+month]

        model.storage_lower_bound = Param(model.storage_nodes, model.rem_months, initialize=min_storage)

        model.storage_avg = Param(model.storage_nodes, model.rem_months, initialize=avg_storage)

        model.initial_storage = Param(model.storage_nodes, initialize=initial_storage)

        current_demand_forecast = {}
        current_month = self.network.current_timestep.month
        for node in self.nodes:
            if node.component_type != 'JVFarmAgentSimple' and node.component_type != 'JVFarmAgent':
                if month == 12:
                    for m in range(12):
                        if node.component_type == 'AgDemand':
                            current_demand_forecast[(node.name, m+1)] = self.demand_forecast_per_node[(node.name, m+1)]
                        elif node.component_type == 'Reservoir':
                             current_demand_forecast[(node.name, m+1)] = 10000000000
                             # Allows reservoirs to spill, but does not count towards objective function
                        else:
                            current_demand_forecast[(node.name, m+1)] = 0
                else:
                    for m in range(12-month):
                        if node.component_type == 'AgDemand':
                            current_demand_forecast[(node.name, month+m)] = self.demand_forecast_per_node[(node.name, month+m)]
                        elif node.component_type == 'Reservoir':
                             current_demand_forecast[(node.name, m+month)] = 10000000000  # Allows reservoirs to spill
                        else:
                            current_demand_forecast[(node.name, m+month)] = 0

        model.demand = Param(model.nodes, model.rem_months, initialize=current_demand_forecast)

        storage_pos_penalty_coefficient = {
            'talal_res': 1,
            'shueib_res': 1,
            'arab_res': 1,
            'shurabil_res': 1,
            'kafrein_res': 1,
            'karameh_res': 1
        }

        model.storage_pos_penalty = Param(model.storage_nodes, initialize=storage_pos_penalty_coefficient)

        storage_neg_penalty_coefficient = {
            'talal_res': 1,
            'shueib_res': 1,
            'arab_res': 1,
            'shurabil_res': 1,
            'kafrein_res': 1,
            'karameh_res': 1
        }

        model.storage_neg_penalty = Param(model.storage_nodes, initialize=storage_neg_penalty_coefficient)

        # Define Decision Variables
        model.Q = Var(model.links, model.rem_months, domain=NonNegativeReals)  # Link flows
        model.delivery = Var(model.nodes, model.rem_months, domain=NonNegativeReals)  # Demand node deliveries
        model.release = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)  # Reservoir releases
        model.storage = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)  # Reservoir storage
        model.storage_plus = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)
        model.storage_minus = Var(model.storage_nodes, model.rem_months, domain=NonNegativeReals)

        # Define Objective Function
        def objective_function(model):
            """
            Objective function for JVA Pyomo optimization: Minimizes deficit between target delivery and actual deliver
            """
            return (sum(model.demand[node, month] - model.delivery[node, month] for node in model.demand_nodes
                        for month in model.rem_months) / \
                    (sum(model.demand[node, month] for node in model.demand_nodes for month in model.rem_months))) + \
                   (sum(model.storage_neg_penalty[node] * model.storage_minus[node, month]
                        for node in model.storage_nodes for month in model.rem_months) /
                    (sum(model.storage_avg[node, month] for node in model.storage_nodes for month in model.rem_months)))


        model.Z = Objective(rule=objective_function, sense=minimize)

        # Define Constraints
        # Mass Balance constraints for nodes

        model.incoming_links = None
        model.outgoing_links = None

        # Mass balance for non-storage nodes
        def m_mass_balance_demand(model, node, month):
            """
            Constraint for mass balance over nodes. Within a month, ensures: Inflows = Outflows - Consumption
            """

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            term1 = sum([model.Q[n1, n2, month] -
                         (model.Q[n1, n2, month] * model.cost[(n1, n2)] * .000006 *
                          self.network.parameters.jva['jva_nrw_factor']) for n1, n2 in nodes_in])
            term2 = model.inflow[node, month]
            # deficit

            #  exports
            term4 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_out])
            # demand
            term5 = model.delivery[node, month]
            # exog outflows (used for zai transfer)
            term6 = model.exog_outflow[node, month]
            # surplus

            return (term1 + term2) - (term4 + term5 + term6) == 0

        model.m_mass_balance_demand_const = Constraint(model.non_storage_nodes, model.rem_months,
                                                       rule=m_mass_balance_demand)

        # Mass balance for storage nodes
        def m_mass_balance_storage(model, node, month):
            """
            Constraint for mass balance over storage nodes. Within a month, ensures: Release = Link Outflow
            """

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in  = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            # imports
            term1 = model.release[node, month]

            #  exports
            term4 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_out])

            return term1 - term4 == 0

        model.m_mass_balance_storage = Constraint(model.storage_nodes, model.rem_months, rule=m_mass_balance_storage)

        # Mass balance for storage +/- separation

        def res_balance_plus_minus(model, node, month):

            return model.storage[node, month] == model.storage_avg[node, month] + model.storage_plus[node,month] - model.storage_minus[node, month]

        model.res_balance_plus_minus = Constraint(model.storage_nodes, model.rem_months, rule=res_balance_plus_minus)

        # Mass balance for storage nodes over time

        def res_balance_storage(model, node, month):
            """
            Constraint for mass balance at storage nodes over time.
            """

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in  = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            if month == self.network.current_timestep.month:
                return model.storage[node, month] == model.initial_storage[node]
            elif self.network.current_timestep.month == 12:
                if month == 1:
                    return model.storage[node, month] - model.storage[node, 12] + model.release[node, 12] - \
                           model.inflow[node, 12] - sum([model.Q[n1, n2, 12] -
                                  (model.Q[n1, n2, 12] * model.cost[(n1,n2)] *.000006 *
                                   self.network.parameters.jva['jva_nrw_factor']) for n1, n2 in nodes_in]) + \
                           model.delivery[node, 12] + model.evap[node, 12] + model.seepage[node, 12] == 0
                else:
                    return model.storage[node, month] - model.storage[node, month-1] + model.release[node, month-1] - \
                           model.inflow[node, month-1] - \
                           sum([model.Q[n1, n2, month-1] - (model.Q[n1, n2, month-1] * model.cost[(n1,n2)] *.000006 *
                                                            self.network.parameters.jva['jva_nrw_factor'])
                                for n1, n2 in nodes_in]) + model.delivery[node, month-1] \
                          + model.evap[node, month-1] + model.seepage[node, month-1] == 0
            else:
                return model.storage[node, month] - model.storage[node, month-1] + model.release[node, month-1] - \
                       model.inflow[node, month-1] - \
                       sum([model.Q[n1, n2, month-1] - (model.Q[n1, n2, month-1] * model.cost[(n1,n2)] *.000006 *
                                                        self.network.parameters.jva['jva_nrw_factor'])
                            for n1, n2 in nodes_in]) + model.delivery[node, month-1] \
                          + model.evap[node, month-1] + model.seepage[node, month-1] == 0

        model.res_balance_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_balance_storage)

        def smooth_deficit_month_lesser_v2(model, node, month, other_month):
            """
                Lesser than constraint to smooth out deficits over time. Normalized deficit between successive
                months within 10 percent
            """

            if other_month == month or model.demand[node, month] == 0 or model.demand[node, other_month] == 0 or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] <= 1.1 * \
                       (model.demand[node, other_month] - model.delivery[node, other_month]) / model.demand[node, other_month]

        model.smooth_deficit_month_lesser_v2 = Constraint(model.demand_nodes, model.rem_months, model.rem_months,
                                                          rule=smooth_deficit_month_lesser_v2)

        def smooth_deficit_month_greater_v2(model, node, month, other_month):
            """
                Greater than constraint to smooth out deficits over time. Normalized deficit between successive months
                within 10 percent
            """

            if other_month == month or model.demand[node, month] == 0 or model.demand[node, other_month] == 0 or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] >= 0.9 * \
                       (model.demand[node, other_month] - model.delivery[node, other_month]) / model.demand[node, other_month]

        model.smooth_deficit_month_greater_v2 = Constraint(model.demand_nodes, model.rem_months, model.rem_months,
                                                           rule=smooth_deficit_month_greater_v2)

        def smooth_deficit_past_month_lesser_v2(model, node, month, past_month):
            """
                Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months
                within 10 percent
            """

            if current_month == 12 or self.network.current_timestep_idx == 0 or model.demand[node,month] == 0 or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] <= 1.1 * \
                       model.past_def[node,past_month]


        def smooth_deficit_past_month_greater_v2(model, node, month, past_month):
            """
                Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months
                within 10 percent
            """

            if current_month == 12 or self.network.current_timestep_idx == 0 or model.demand[node,month] == 0 or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] >= 0.9 * \
                       model.past_def[node,past_month]


        model.smooth_deficit_past_month_greater_v2 = Constraint(model.demand_nodes, model.rem_months, model.past_months,
                                                                rule=smooth_deficit_past_month_greater_v2)

        # Deficit over demand nodes smoothed for each month
        def smooth_deficit_node_lesser(model, node, other_node, month):
            """
                Lesser than constraint to smooth out deficits over nodes. Normalized deficit between successive months
                within 20 percent
            """
            if other_node == node or model.demand[node, month] == 0 or model.demand[other_node, month] == 0 or \
                    other_node == 'so10' or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] <= 1.1 * \
                       (model.demand[other_node, month] - model.delivery[other_node, month]) / model.demand[other_node, month]

        model.smooth_deficit_node_lesser = Constraint(model.demand_nodes, model.demand_nodes, model.rem_months,
                                                      rule=smooth_deficit_node_lesser)

        def smooth_deficit_node_greater(model, node, other_node, month):
            """
                Greater than constraint to smooth out deficits over nodes. Normalized deficit between successive months
                within 20 percent
            """
            if other_node == node or model.demand[node, month] == 0 or model.demand[other_node, month] == 0 or \
                    other_node == 'so10' or node == 'so10':
                return Constraint.Skip
            else:
                return (model.demand[node, month] - model.delivery[node, month]) / model.demand[node, month] >= 0.9 * \
                       (model.demand[other_node, month] - model.delivery[other_node, month]) / model.demand[other_node, month]

        model.smooth_deficit_node_greater = Constraint(model.demand_nodes, model.demand_nodes, model.rem_months,
                                                       rule=smooth_deficit_node_greater)

        # Storage deficit per storage node smoothed out over time horizon

        def smooth_negstorage_month_lesser(model, node, month, other_month):
            """
                Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months
                within 10 percent
            """

            if other_month == month or node == 'kafrein_res' or node == 'karameh_res':
                return Constraint.Skip
            else:
                return model.storage_minus[node, month] / model.storage_avg[node, month] <= 2 * \
                       model.storage_minus[node, other_month] / model.storage_avg[node, other_month]

        model.smooth_negstorage_month_lesser = Constraint(model.storage_nodes, model.rem_months, model.rem_months,
                                                          rule=smooth_negstorage_month_lesser)

        def smooth_negstorage_month_greater(model, node, month, other_month):
            """
                Lesser than constraint to smooth out deficits over time. Normalized deficit between successive months
                within 10 percent
            """

            if other_month == month or node == 'kafrein_res' or node == 'karameh_res':
                return Constraint.Skip
            else:
                return model.storage_minus[node, month] / model.storage_avg[node, month] >= 0.5 * \
                       model.storage_minus[node, other_month] / model.storage_avg[node, other_month]

        model.smooth_negstorage_month_greater = Constraint(model.storage_nodes, model.rem_months, model.rem_months,
                                                           rule=smooth_negstorage_month_greater)

        # Storage deficit over storage nodes smoothed for each month

        def smooth_negstorage_node_lesser(model, node, other_node, month):
            """
                Lesser than constraint to smooth out deficits over nodes. Normalized deficit between successive months
                within 20 percent
            """
            if other_node == node or node == 'kafrein_res' or node == 'karameh_res' or other_node == 'kafrein_res' or \
                    other_node == 'karameh_res':
                return Constraint.Skip
            else:
                return model.storage_minus[node, month] / model.storage_avg[node, month] <= 2 * \
                       model.storage_minus[other_node, month] / model.storage_avg[other_node, month]

        model.smooth_negstorage_node_lesser = Constraint(model.storage_nodes, model.storage_nodes, model.rem_months,
                                                         rule=smooth_negstorage_node_lesser)

        def smooth_negstorage_node_greater(model, node, other_node, month):
            """
                Lesser than constraint to smooth out deficits over nodes. Normalized deficit between successive months
                within 20 percent
            """
            if other_node == node or node == 'kafrein_res' or node == 'karameh_res' or other_node == 'kafrein_res' or \
                    other_node == 'karameh_res':
                return Constraint.Skip
            else:
                return model.storage_minus[node, month] / model.storage_avg[node, month] >= 0.5 * \
                       model.storage_minus[other_node, month] / model.storage_avg[other_node, month]

        model.smooth_negstorage_node_greater = Constraint(model.storage_nodes, model.storage_nodes, model.rem_months,
                                                          rule=smooth_negstorage_node_greater)

        # Max release criteria
        #
        def res_max_release(model, node, month):
            """
            Constraint for max release at a reservoir. Based on historically observed max releases
            """
            return model.release[node, month] <= model.max_release[node]

        model.res_max_release = Constraint(model.storage_nodes, model.rem_months, rule=res_max_release)

        # Reservoir minimum storage

        def res_minimum_storage(model, node, month):
            """
            Constraint for min storage at a reservoir. Based on historically observed min storage
            """
            # return model.storage[node, month] >= model.storage_lower_bound[node, month]
            return model.storage[node, month] >= 0

        model.res_minimum_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_minimum_storage)
        #
        # Reservoir maximum storage

        def res_maximum_storage(model, node, month):
            """
            Constraint for max storage at a reservoir. Based on reservoir storage capacity
            """
            return model.storage[node, month] <= model.storage_upper_bound[node]

        model.res_maximum_storage = Constraint(model.storage_nodes, model.rem_months, rule=res_maximum_storage)

        # Delivery does not exceed demand

        def delivery_capacity(model, node, month):
            """
            Constraint to prevent delivery from exceeding target demand
            """
            return model.delivery[node, month] <= model.demand[node, month]

        model.delivery_capacity = Constraint(model.non_storage_nodes, model.rem_months, rule=delivery_capacity)

        # Storage deficit for a given month balanced out between storage nodes

        opt = SolverFactory("ipopt", solver_io='nl')
        instance = model.create_instance()
        instance.dual = Suffix(direction=Suffix.IMPORT)
        try:
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "ipopt"
            solver_status = "ok"
        except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
            solver_status = "ipopt_failed"
        if solver_status != "ipopt_failed":
            if (results.solver.status == SolverStatus.ok) and (
                        results.solver.termination_condition == TerminationCondition.optimal):
                pass
            else:
                logging.info("jva failed to solve with ipopt ... retrying")
                opt = SolverFactory("cplex")
                instance = model.create_instance()
                instance.dual = Suffix(direction=Suffix.IMPORT)
                results = opt.solve(instance)
                model.solutions.load_from(results)
                # logging.info(results)
                solver_name = "cplex"
        else:
            logging.info("jva failed to solve with ipopt ... retrying")
            opt = SolverFactory("cplex")
            instance = model.create_instance()
            instance.dual = Suffix(direction=Suffix.IMPORT)
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "cplex"

        if (results.solver.status == SolverStatus.ok) and (
                    results.solver.termination_condition == TerminationCondition.optimal):
            logging.info("jva solved using " + solver_name)
        else:
            logging.info("jva failed to solve")

        # Load results from pyomo optimization to associated pynsim component attributes

        self.node_delivery_history.append({})
        self.node_allocations_history.append({})

        for var in model.component_objects(Var, active=True):
            var = str(var)
            if var == "storage":
                p_var = getattr(instance, var)
                total=0
                for vv in p_var:
                    name= ''.join(map(str,vv))
                    total=total+p_var[vv].value
                    # Set reservoir node storage value to optimization result (for intiial timestep,
                    #  equals storage at period 1, otherwise equals storage at period 2)
                    if self.network.current_timestep_idx == 0:
                        if vv[1] == self.network.current_timestep.month:
                            self.get_node(str(vv[0])).volume = p_var[vv].value
                    else:
                        if self.network.current_timestep.month == 12:
                            if vv[1] == 1:
                                self.get_node(str(vv[0])).volume = p_var[vv].value
                        else:
                            if vv[1] == self.network.current_timestep.month + 1:
                                self.get_node(str(vv[0])).volume = p_var[vv].value


            elif var=="release":
                d_var = getattr(instance, var)
                total=0
                for vv in d_var:
                    name= ''.join(map(str,vv))
                    total=total+d_var[vv].value
                    # Set reservoir node release value to optimization result (for month 1 only)
                    if vv[1] == self.network.current_timestep.month:
                        self.get_node(str(vv[0])).release = d_var[vv].value
            elif var=="delivery":
                d_var = getattr(instance, var)
                total=0
                for vv in d_var:
                    name= ''.join(map(str,vv))
                    if vv[0][0:2] == 'so':
                        demand = self.demand_forecast_per_node[vv]
                        deficit = demand - d_var[vv].value
                        percent_diff = (deficit/demand) if (demand > 0) else 0
                    self.node_allocations[vv] = d_var[vv].value
                    self.node_allocations_history[-1][vv] = d_var[vv].value
                    if vv[1] == self.network.current_timestep.month:
                        self.node_delivery_history[-1][vv[0]] = d_var[vv].value
                        self.node_delivery[vv[0]] = d_var[vv].value
            elif var=="Q":
                q_var = getattr(instance, var)
                total=0
                for vv in q_var:
                    name= "(" + ', '.join(map(str,vv)) + ")"
                    total=total+q_var[vv].value
                    for link in self.links:
                        if link.start_node.name == vv[0] and link.end_node.name == vv[1] and vv[2] == self.network.current_timestep.month:
                            length = link.length
                            # Set link flow attribute equal to optimization result (for current month only)
                            link.flow = q_var[vv].value
                            break


    def get_allocation_model(self):
        """
            Get the pyomo abstract model.
            If it's already been created, return it. If not, create it.
        """

        if self.model is not None:
            return self.model

        # Define lists to initialize model parameter sets
        nodes_names = []
        demand_nodes = []
        storage_nodes = []
        non_storage_nodes = []
        links_comp = {}
        link_length = {}
        max_storage = {}
        max_release = {}

        # Add demand nodes
        for i in self.nodes:
            if i.component_type == 'AgDemand':
                demand_nodes.append(i.name)

        # Add node names
        for node in self.nodes:
            if node.component_type != 'JVFarmAgentSimple' and node.component_type != 'JVFarmAgent':
                nodes_names.append(node.name)

        for link in self.links:
            if link.component_type != 'Nearest':
                links_comp[(link.start_node.name, link.end_node.name)] = link
                link_length[(link.start_node.name, link.end_node.name)] = link.length

        for res in self.nodes:
            if res.component_type == 'Reservoir':
                storage_nodes.append(res.name)
                max_storage[res.name] = res.res_properties['live_storage_capacity']
                max_release[res.name] = res.res_properties['max_release']
            elif res.component_type != 'JVFarmAgentSimple' and res.component_type != 'JVFarmAgent':
                non_storage_nodes.append(res.name)

        # Declaring model
        model = AbstractModel()
        model.months = Set(initialize=range(1, 13))
        model.nodes = Set(initialize=nodes_names)
        model.links = Set(within=model.nodes * model.nodes, initialize=links_comp.keys())
        model.demand_nodes = Set(initialize=demand_nodes)
        model.crop_groups = Set(initialize=self.crop_groups)
        model.storage_nodes = Set(initialize=storage_nodes)
        model.non_storage_nodes = Set(initialize=non_storage_nodes)

        # Declaring model parameters
        # parameters for storage reservoirs
        model.cost = Param(model.links, initialize=link_length)
        model.storage_upper_bound = Param(model.storage_nodes, initialize=max_storage)
        model.max_release = Param(model.storage_nodes, initialize=max_release)

        self.model = model

        return self.model

    def set_mujib_forecast(self):
        mujib = self.network.get_node('mujib_res')
        current_volume = mujib.volume
        current_year = self.network.current_timestep.year
        current_month = self.network.current_timestep.month
        avg_volume = self.network.observations.volume_averages['mujib_res'][current_month]
        balance = current_volume - avg_volume
        avg_seep = mujib.res_properties['avg_seep']
        avg_evap = self.network.observations.evap_averages[mujib.name][current_month]
        for m in range(12):
            if m == 0:
                balance -= avg_seep
                balance -= mujib.evap
                try:
                    if np.isnan(self.network.observations.res_inflow_mujib_proj[current_year][current_month]):
                        balance += self.network.observations.inflow_averages['mujib_res'][current_month]
                    else:
                        balance += self.network.observations.res_inflow_mujib_proj[current_year][current_month]
                except KeyError:
                    balance += self.network.observations.inflow_averages['mujib_res'][current_month]
            else:
                if current_month + m <= 12:
                    balance -= avg_seep
                    balance -= self.network.observations.evap_averages['mujib_res'][current_month+m]
                    balance += self.network.observations.inflow_averages['mujib_res'][current_month+m]
                else:
                    balance -= avg_seep
                    balance -= self.network.observations.evap_averages['mujib_res'][current_month+m-12]
                    balance += self.network.observations.inflow_averages['mujib_res'][current_month+m-12]
        self.mujib_annual_availability = balance

    def set_walah_forecast(self):
        walah = self.network.get_node('walah_res')
        current_volume = walah.volume
        current_year = self.network.current_timestep.year
        current_month = self.network.current_timestep.month
        avg_volume = self.network.observations.volume_averages['walah_res'][current_month]
        balance = current_volume - avg_volume
        avg_seep = walah.res_properties['avg_seep']
        avg_evap = self.network.observations.evap_averages[walah.name][current_month]
        for m in range(12):
            if m == 0:
                balance -= avg_seep
                balance -= walah.evap
                try:
                    if np.isnan(self.network.observations.res_inflow_walah_proj[current_year][current_month]):
                        self.network.observations.inflow_averages['walah_res'][current_month]
                    else:
                        balance += self.network.observations.res_inflow_walah_proj[current_year][current_month]
                except KeyError:
                    balance += self.network.observations.inflow_averages['walah_res'][current_month]
            else:
                if current_month + m <= 12:
                    balance -= avg_seep
                    balance -= self.network.observations.evap_averages['walah_res'][current_month+m]
                    balance += self.network.observations.inflow_averages['walah_res'][current_month+m]
                else:
                    balance -= avg_seep
                    balance -= self.network.observations.evap_averages['walah_res'][current_month+m-12]
                    balance += self.network.observations.inflow_averages['walah_res'][current_month+m-12]
        self.walah_annual_availability = balance

    def setup(self, timestep):
        pass


class WAJ(JordanInstitution):
    """The Water Authority of Jordan class
    
    Determines municipal water extraction and allocation throughout the country. 
    
    WAJ methods are called in 
    
    **Properties**:


        |  *yearly_max_groundwater_pumping* (dict/int) - dictionary of maximum allowed yearly extraction, indexed by
             governorate name
        |  *demand_forecast* (dict/int) - dictionary of anticipated water needs, indexed by governorate name and month
        |  *allocation_forecast* (dict/int) - dictionary of water allocation forecast, indexed by governorate name & month
        |  *pumping_forecast* (dict/int) - dictionary of pumping forecast, indexed by governorate name and month
        
        |  *gov_delivery* (dict/dict/int) - 
        |  *gov_gw_pumping* (dict/int) -
        |  *gov_baseline_pump* (dict/int)



    **Scenario-based inputs**:

        |  *population* (dict/int) - Population


    **Intervention-based inputs**:


    **Agent inputs**:


    **Hydro inputs**:

    """
    description = "Water Authority of Jordan"
    
    def __init__(self, name, **kwargs): 
        super(WAJ, self).__init__(name, **kwargs)

        self.model = None
        self.WAJ_inputs = None

        self.supply_project_wells = None

        self.simulation_inputs = None
        self.writer = None
        self.writer2 = None

        self.population_multiplier=0
        self.total_water_availability = 0

        self.nodes_names=[]
        self.demand_nodes=[]
        self.needs={}
        self.linked_demand_nodes=[]
        self.non_demand_nodes=[]
        self.storage_nodes=[]
        self.nonstorage_nodes=[]
        self.node_initial_storage={}
        self.links_comp={}
        self.min_storage={}
        self.max_storage={}
        self.inflow={}
        self.consumption_coefficient={}
        self.upper_flow={}
        self.lower_flow={}
        self.flow_multiplier={}
        self.cost={}
        self.monthdays=[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        self.is_WAJ_forecast=0
        self.Maan_2013={1:753308,
                2:757196,
                3:850777,
                4:928532,
                5:955123,
                6:1053057,
                7:3866820,
                8:5947062,
                9:6066611,
                10:5921222,
                11:7450817,
                12:7792578}
        model_inputs_xlsx = get_excel_data("model_setup.xlsx")
        simulation_inputs = model_inputs_xlsx.parse("simulation")
        for s in range(simulation_inputs.shape[0]):
            if simulation_inputs.run[s] == 'yes':
                self.startyear=simulation_inputs.start_month[s]
                self.startyear=self.startyear[4:8]
                self.numyears=simulation_inputs.number_of_years[s]
                self.startyear=int(self.startyear)
                self.numyears=int(self.numyears)

    _properties = {
        'log':[],
        'population': {},
        'pop_growth': 0.02,
        'population_estimate': {},
        'demand_estimate': {},
        'gw_pumping': {},
        'disi_cap': 11000000,
        'fail_cap': 1500000,
        'yearly_max_groundwater_pumping':{},
        'demand_forecast':{},
        'initial_step':1,

        'gw_pumping_forecast': {},  # pumping forecasts (time series list), indexed by unit stress location (gw node)
        'gov_forecast': {},  # allocation forecasts for next 12 months (time series list), indexed by governorate name
        'demand_request': 0,  # consolidated demand requests of governorates
        'gov_delivery': {},
        'gov_gw_pumping':{},  # governorate keys
        'gov_baseline_pump':{},  # governorate keys
        'elec_tariff':0,  # JD/kWh
        'real_transfers':{},
        'real_extraction':{},
        # Non-revenue water properties:
        'NRW_ratio': {},
        'physical_losses_ratio': {},
        'physical_losses_volume': {},
        'admin_losses_ratio': {},
        'admin_losses_volume': {},
        'gov_pop':{},
        'year_number':{},  # a counter for years from simulation start
        'gov_list': ['ajloun', 'amman', 'aqaba', 'balqa', 'irbid', 'jarash', 'karak', 'maan', 'madaba', 'mafraq',
                     'tafilah', 'zarqa'],
        'consumer_compensation' : {},
        'gov_surplus_prev_year':{},
        'population': {},
        'total_lifeline': 0,
    }

    def actualize_wells_share(self, timestep):
        
        """
         Updates the disaggregation of groundwater extraction into GW nodes
        
         WAJ solves the allocation problem at governorate levels. It then needs to compute actual extractions at the
           subdistrict level.
        """
        
        # I. Initialize when starting simulation: calculate wells share from 2009 baseline
        if self.initial_step==1:
            self.initial_step=0
            self.wells_initial_ratio={}
            self.wells_current_ratio={}
            self.gov_baseline_pump={}
            self.gov_missing_ratio={}
            self.gov_baseline_country_ratio={}

            for i in self.network.institutions:
                if i.component_type == 'GOV':
                    sum_gov_pump = 0
                    for g in self.nodes:
                        if g.component_type == 'Groundwater':
                            if g._gw_node_properties['gov']==i.name:
                                sum_gov_pump = sum_gov_pump + g._gw_node_properties['baseline_pumping']*3600*24*\
                                               self.monthdays[timestep.month-1]
                    self.gov_baseline_pump[i.name]= sum_gov_pump
                    self.gov_missing_ratio[i.name]=0
            
            for i in self.network.institutions:
                if i.component_type == 'GOV':
                    self.gov_baseline_country_ratio[i.name]=self.gov_baseline_pump[i.name]/\
                                                            sum(self.gov_baseline_pump.itervalues())

                    for g in self.nodes:
                        if g.component_type == 'Groundwater':
                            if g._gw_node_properties['gov']==i.name:
                                
                                gw_node_previous_pump =g._gw_node_properties['baseline_pumping']*3600*24*\
                                                       self.monthdays[timestep.month-1]
                                gov_previous_pump=self.gov_baseline_pump[i.name] 
                                self.wells_initial_ratio[g.name] =gw_node_previous_pump/gov_previous_pump
        
        # II: Update wells share if some are dry
        self.wells_current_ratio={}
        self.gov_missing_ratio={}
        self.gov_missing_volume={}
        self.gov_healthy_ratio={}


        # 1. calculate the volume to re-distribute over remaining wells
        for i in self.network.institutions:
            if i.component_type == 'GOV':
                missing_ratio=0
                healthy_ratio=0
                for g in self.nodes:
                    if g.component_type == 'Groundwater':
                        if g._gw_node_properties['gov'] == i.name:
                            if g.capacity_reduction_factor < 1:
                                self.log.append(['well '+str(g.name)+' ('+i.name+') is below capacity'])
                            missing_ratio += self.wells_initial_ratio[g.name]*(1-g.capacity_reduction_factor)
                            self.wells_current_ratio[g.name]=self.wells_initial_ratio[g.name]*g.capacity_reduction_factor
                            healthy_ratio+=self.wells_current_ratio[g.name]
                self.gov_missing_ratio[i.name]=missing_ratio
                self.gov_healthy_ratio[i.name]=healthy_ratio
                self.gov_missing_volume[i.name]=self.gov_missing_ratio[i.name]*self.yearly_max_groundwater_pumping[i.name]

        # 2 if required, modify the yearly extraction target according to wells state:
        country_missing_extraction=sum(self.gov_missing_volume.itervalues())
        self.log.append(['missing extraction:'+str(country_missing_extraction)])

        # 2.a Calculate the extraction shares, relative to the country, of each governorate that still has healthy wells
        healthy_gov_list=[]
        gov_extraction_share={}
        for i in self.network.institutions:
            if i.component_type == 'GOV':
                N_healthywells=0
                for g in self.nodes:
                    if g.component_type == 'Groundwater' and g._gw_node_properties['gov']==i.name:
                        if g.capacity_reduction_factor==1:
                            N_healthywells+=1
                if N_healthywells>0:
                    healthy_gov_list.append(i.name)
                else:
                    logging.info(i.name + ' has no healthy wells left')
                    self.log.append([i.name + ' has no healthy wells left'])
        total_healthy_extraction=sum(self.yearly_max_groundwater_pumping[name] for name in healthy_gov_list)

        if total_healthy_extraction==0:
            logging.info('Result warning: Jordan has no healthy wells left')
            self.log.append('Jordan has no healthy wells left')

        for name in healthy_gov_list:
            gov_extraction_share[name]=self.yearly_max_groundwater_pumping[name]/total_healthy_extraction

        previous_GW_pumping=sum(self.yearly_max_groundwater_pumping[name] for name in self.gov_list)

        # 2.b modify extraction target and capacity
        for i in self.network.institutions:
            if i.component_type == 'GOV':
                if self.is_WAJ_forecast == 1:
                    self.yearly_max_groundwater_pumping[i.name]=self.yearly_max_groundwater_pumping[i.name]-self.gov_missing_volume[i.name]
                    for month in range(1,13):
                        self.extraction_cap[i.name,month]=self.extraction_cap[i.name,month]*(1-self.gov_missing_ratio[i.name])

        new_total_GW_pumping=sum(self.yearly_max_groundwater_pumping[name] for name in self.gov_list)
        if abs(new_total_GW_pumping - previous_GW_pumping) > 1000:
             self.log.append(['High GW extraction reallocation value. Healthy gov list:'])
             self.log.append(healthy_gov_list)

       # 3. in each governorate, rearrange the share of each well to the governorate extraction according to reduction
       #  factors if there are no healthy wells left in the governorate, its total production declines.
        for i in self.network.institutions:
            if i.component_type == 'GOV':
                sum_ratios=0
                for g in self.nodes:
                    if g.component_type == 'Groundwater':
                        if g._gw_node_properties['gov']==i.name:
                            if self.gov_missing_ratio[i.name]>=1:
                                self.wells_current_ratio[g.name] = 0

                            if self.gov_healthy_ratio[i.name] > 0:
                                self.wells_current_ratio[g.name] = self.wells_initial_ratio[g.name] * \
                                    g.capacity_reduction_factor * (1. / self.gov_healthy_ratio[i.name])
                            else:
                                self.wells_current_ratio[g.name] = 0

                            sum_ratios+=self.wells_current_ratio[g.name]
                if sum_ratios>1.01 or sum_ratios<0.99:
                    self.log.append(['Full decline or incorrect well ratios ('+i.name+'), sum ='+str(sum_ratios)+
                                     ' remaining healthy share = '+str(self.gov_healthy_ratio[i.name])])

        if self.is_WAJ_forecast == 1:
            if self.supply_project_wells is None:
                self.supply_project_wells = get_excel_data("WAJ_inputs.xlsx").parse("supply_project_wells")
            for i in self.supply_project_wells:
                wells = [self.get_node(w) for w in self.supply_project_wells[i] if str(w) != 'nan']
                baseline_pumping_sum = sum(w._gw_node_properties['baseline_pumping'] for w in wells)
                gw_reduction = sum(w.capacity_reduction_factor * w._gw_node_properties['baseline_pumping']
                                   for w in wells) / baseline_pumping_sum
                self.yearly_max_groundwater_pumping[i] = self.yearly_max_groundwater_pumping[i] * gw_reduction
                for month in range(1, 13):
                    self.extraction_cap[i, month] = self.extraction_cap[i, month] * gw_reduction

        return                    

            
    def set_yearly_max_groundwater_pumping(self, timestep):
        """
        WAJ determines maximum yearly resource availability. Currently reads from external excel file.
        
        Adapts to dry or drying wells.

        """
        self.export_data=0  # self.network.parameters.waj['export_results']

        if timestep.year>=2017:
            self.get_link('amman_zarqa').max_flow=10000000
            self.get_link('zarqa_amman').max_flow=10000000
            
        self.extraction_cap={}
        self.elec_tariff=self.network.parameters.waj['electricity_tariff']
        self.SWR=self.network.parameters.waj['surface_water_reduction']
        self.yearly_GW_max_scale=self.network.parameters.waj['GW_cap_scale']
        
        cwa_allocation = self.network.get_institution('cwa').waj_forecast
        if self.WAJ_inputs is None:
            self.WAJ_inputs = get_excel_data("WAJ_inputs.xlsx")

        if timestep.year<2015:
            maxpump_DF=self.WAJ_inputs.parse('tot_extraction'+str(timestep.year))
        else:
            if self.network.parameters.waj['WAJ_demand'] == 'obs_targets_per_cap':
                maxpump_DF=self.WAJ_inputs.parse('tot_extraction2015')
            else:
                maxpump_DF=self.WAJ_inputs.parse('tot_extraction2014')

        delayed_sources_DF=self.WAJ_inputs.parse('delayed_sources')

        self.yearly_max_groundwater_pumping=maxpump_DF.set_index('name')['limit'].to_dict()

        ######################################################################
        # Tanker analysis investment 1: Supply augmentation - Part A - START #
        ######################################################################
        if self.network.simulation_type == "tanker":
            if self.network.parameters.tanker['tanker_analysis_investment_1'] == 1:
                self.network.parameters.sup_int["tiberias_RSDS_2"] = u'yes'
                self.network.parameters.sup_int["aqa_RSDS"] = u'yes'
                self.network.parameters.sup_int["RSDS_ph2"] = u'yes'

                self.network.parameters.waj['WAJ_demand'] = 'obs_targets_per_cap'
                maxpump_DF = self.WAJ_inputs.parse('tot_extraction2015')
                self.yearly_max_groundwater_pumping = maxpump_DF.set_index('name')['limit'].to_dict()
                self.yearly_max_groundwater_pumping["tiberias_RSDS_2"] = 45000000.
                self.yearly_max_groundwater_pumping["aqa_RSDS"] = 30000000.
                self.yearly_max_groundwater_pumping["RSDS_ph2"] = 225000000.
        ####################################################################
        # Tanker analysis investment 1: Supply augmentation - Part A - END #
        ####################################################################

        self.delayed_sources=delayed_sources_DF.set_index('name')['year'].to_dict()
        if self.network.parameters.waj['delayed_sources_phase_in'] == 1.0:
            delayed_sources_phase_in_DF = self.WAJ_inputs.parse('delayed_sources_phase_in')
            self.delayed_sources_phase_in = delayed_sources_phase_in_DF.set_index("name")[timestep.year].to_dict()

        # include JVA forecasts for Mujib and Wala
        walah=self.network.get_institution('jva').walah_annual_availability
        mujib=self.network.get_institution('jva').mujib_annual_availability
        zara_maeen_remain=self.yearly_max_groundwater_pumping['zara_maeen']
        self.yearly_max_groundwater_pumping['zara_maeen']=(zara_maeen_remain+walah+mujib)*(1+self.SWR)

        extr_cap_DF=self.WAJ_inputs.parse('Extraction_cap')

        ######################################################################
        # Tanker analysis investment 1: Supply augmentation - Part B - START #
        ######################################################################
        if self.network.simulation_type == "tanker":
            if self.network.parameters.tanker['tanker_analysis_investment_1'] == 1:
                self.network.parameters.waj['delayed_sources_phase_in'] = 1.0
                delayed_sources_phase_in_DF = self.WAJ_inputs.parse('delayed_sources_phase_in')
                self.delayed_sources_phase_in = delayed_sources_phase_in_DF.set_index("name")[timestep.year].to_dict()
                print("Tanker water market analyses - supply augmentation:")
                if timestep.year >= 2025:
                    self.delayed_sources_phase_in["aqa_RSDS"] = 1L
                    if timestep.year >= 2027:
                        print(" - Phase 2 of 2 completed: +300 million m3/a")
                        self.delayed_sources_phase_in["tiberias_RSDS_2"] = 1L
                        self.delayed_sources_phase_in["RSDS_ph2"] = 1L
                    else:
                        print(" - Phase 1 of 2 completed: +30 million m3/a")
                else:
                    print(" ... to be activated in model years 2025-2027")
                self.delayed_sources["tiberias_RSDS_2"] = 2027L
                self.delayed_sources["aqa_RSDS"] = 2025L
                self.delayed_sources["RSDS_ph2"] = 2027L

                extr_cap_DF.loc[(extr_cap_DF["name"] == "tiberias_RSDS_2")] = ["tiberias_RSDS_2"] + [45000000. / 12.] * 12
                extr_cap_DF.loc[(extr_cap_DF["name"] == "aqa_RSDS")] = ["aqa_RSDS"] + [30000000. / 12.] * 12
                extr_cap_DF.loc[(extr_cap_DF["name"] == "RSDS_ph2")] = ["RSDS_ph2"] + [225000000. / 12.] * 12
        ####################################################################
        # Tanker analysis investment 1: Supply augmentation - Part B - END #
        ####################################################################

        # Update wells ratios if some are reduced
        for k in self.yearly_max_groundwater_pumping.keys():
            self.yearly_max_groundwater_pumping[k]=self.yearly_max_groundwater_pumping[k]*(1+self.total_water_availability)
            if k=='zai':
                self.yearly_max_groundwater_pumping[k]=sum(self.network.get_institution('cwa').kac_to_zai_forecast)

        if self.network.parameters.waj['delayed_sources_phase_in'] == 1.0:
            for source in self.delayed_sources_phase_in:
                self.yearly_max_groundwater_pumping[source] *= self.delayed_sources_phase_in[source]
        else:
            for source in self.delayed_sources:
                if timestep.year<self.delayed_sources[source]:
                    self.yearly_max_groundwater_pumping[source]=0

        for source in self.network.parameters.sup_int:
            if self.network.parameters.sup_int[source] == 'no':
                self.yearly_max_groundwater_pumping[source] = 0

        for d in extr_cap_DF.index:
            node_name = extr_cap_DF.iloc[d,0]
            for month in range(1,13):  # geometric demand growth:
                if node_name=='zara_maeen' or node_name=='zai':
                    self.extraction_cap[(node_name,month)]=extr_cap_DF.iloc[d,month]
                else:
                    self.extraction_cap[(node_name,month)]=extr_cap_DF.iloc[d,month]
                if node_name=='maan':
                    if timestep.year > 2013:
                        self.extraction_cap[(node_name,month)]=self.disi_cap+extr_cap_DF.iloc[d,month]
                    elif timestep.year == 2013 and month >= 7:
                        self.extraction_cap[(node_name,month)]=self.disi_cap+extr_cap_DF.iloc[d,month]
       
        self.is_WAJ_forecast=1
        self.actualize_wells_share(timestep)
        self.is_WAJ_forecast=0

        if self.network.parameters.waj['passive_WAJ'] == 1:
           return

        NRW=self.WAJ_inputs.parse("NRW")[0:12]        
        self.physical_losses_ratio=NRW["physical_losses"]
        
        # determine extraction costs
        self.gov_extraction_costs={}

        gravity = 9.8  
        water_density = 1000 
        pumping_efficiency = 0.8
        calc_coefficient = water_density*gravity/pumping_efficiency

        for i in self.network.institutions:
            if i.component_type == 'GOV':
                gov_pump_cost = 0
                
                for g in self.nodes:
                    if g.component_type == 'Groundwater':
                        if g._gw_node_properties['gov'] == i.name:
                            gw_node_ratio =self.wells_current_ratio[g.name]
                            
                            head=g.lift
                            unit_cost=calc_coefficient*head/1000*self.elec_tariff  # [kWh*tariff(JD/kWh)]
                            weighted_unit_cost=unit_cost*gw_node_ratio
                            gov_pump_cost+=weighted_unit_cost                            
                            
                self.gov_extraction_costs[i.name]= gov_pump_cost

    def set_demand_forecast(self, timestep):
        """
        WAJ determines water target for the next 12 months for each governorate.

        Current methods, set in paramter_inputs excel file:

        passive_WAJ = forces WAJ to deliver exactly the observed supply, without considering
        actual availability (no mass balance)


        """         
        self.demand_forecast={}
        self.total_demand={}
        self.baseline_year=2011
        if self.network.parameters.waj['passive_WAJ']==1:
            return
        
        cwa_allocation = self.network.get_institution('cwa').waj_forecast

        for gov in self.institutions:
            if gov.component_type == 'GOV':
                self.population[gov.name] = 0

                for hh in self.network.get_institution('human_agent_wrapper').hh_agents:
                    if hh.gov_name == gov.name[0:3]:
                        if np.isnan(hh.represented_units * hh.params['hnum']) or np.isinf(hh.represented_units * hh.params['hnum']):
                            pass
                        else:
                            self.population[gov.name] += hh.represented_units * hh.params['hnum']
                for rf in self.network.get_institution('human_agent_wrapper').rf_agents:
                    if rf.gov_name == gov.name[0:3]:
                        if np.isnan(rf.represented_units * rf.params['hnum']) or np.isinf(rf.represented_units * rf.params['hnum']):
                            pass
                        else:
                            self.population[gov.name] += rf.represented_units * rf.params['hnum']

        self.baseline_pop={'amman':	2419600,
        'aqaba':	136200,
        'balqa':	418600,
        'karak':	243700,
        'maan':	118800,
        'madaba':	156300,
        'mafraq':	293700,
        'jarash':	187500,
        'ajloun':	143700,
        'irbid':	1112300,
        'tafilah':	87500,
        'zarqa':	931100}
        
        for i in self.population.keys():
            self.needs[i]=self.population[i]*10

        if self.WAJ_inputs is None:
            self.WAJ_inputs = get_excel_data("WAJ_inputs.xlsx")

        if timestep.year > 2014:
            demandDF = self.WAJ_inputs.parse('real_supply2014')[0:12]
        else:
            demandDF=self.WAJ_inputs.parse('real_supply'+str(timestep.year))[0:12]

        supply2011DF=self.WAJ_inputs.parse('real_supply2011')[0:12]
        baseline_year_supplyDF=self.WAJ_inputs.parse('real_supply'+str(self.baseline_year))[0:12]
        self.observed_supply={}
        self.observed_supply2011={}
        self.yearly_supply={}
        self.yearly_supply2011={}
        self.baseline_year_supply={}
        
        for d in demandDF.index: 
            gov_name = demandDF.iloc[d,0]
            yearly_supply=0 
            yearly_supply2011=0
            for month in range(1,13):
                self.observed_supply[(gov_name,month)]=demandDF.iloc[d,month]
                self.observed_supply2011[(gov_name,month)]=supply2011DF.iloc[d,month]
                self.baseline_year_supply[(gov_name,month)]=baseline_year_supplyDF.iloc[d,month]
                
                yearly_supply+=self.observed_supply[(gov_name,month)]
                yearly_supply2011+=self.observed_supply2011[(gov_name,month)]
                
            self.yearly_supply[gov_name]=yearly_supply    
            self.yearly_supply2011[gov_name]=yearly_supply2011

        # +++ Demand forecast +++
        for d in demandDF.index:   
            gov_name = demandDF.iloc[d,0]
            if gov_name=='amman':
                target=120
            else:
                target=100

            if self.network.parameters.waj['WAJ_demand'] == 'obs_targets_per_cap':
                targets = self.WAJ_inputs.parse('targets_per_capita')[0:12]
                target = targets.loc[targets["name"] == gov_name].values[0]

            for month in range(1,13): 
                if self.network.parameters.waj['test_equity']==1:
                    self.demand_forecast[(gov_name,month)]=demandDF.iloc[d,month]
                    
                elif self.network.parameters.waj['WAJ_demand']=='observed_supply':
                    self.demand_forecast[(gov_name,month)]=demandDF.iloc[d,month]
                    
                elif self.network.parameters.waj['WAJ_demand']=='MWI_target':
                    self.demand_forecast[(gov_name,month)]=self.population[gov_name]*target*\
                                                           self.observed_supply2011[(gov_name,month)]/\
                                                           self.yearly_supply2011[gov_name]

                elif self.network.parameters.waj['WAJ_demand']=='obs_targets_per_cap':
                    self.demand_forecast[(gov_name,month)] = self.population[gov_name]*target[month]

                elif self.network.parameters.waj['WAJ_demand']=='baseline_popgrowth':
                    baseline_year=self.network.parameters.waj['baseline_year']
                    if timestep.year==baseline_year:
                        self.demand_forecast[(gov_name,month)]=self.baseline_year_supply[(gov_name,month)]
                    else:
                        self.demand_forecast[(gov_name,month)]=\
                            self.baseline_year_supply[(gov_name,month)]*\
                            (1+(self.population[gov_name]-self.baseline_pop[gov_name])/self.population[gov_name])

        for source in self.delayed_sources.keys():
            if timestep.year>=self.delayed_sources[source]:
                shift_DF=self.WAJ_inputs.parse('target_shift_'+source)
                for d in demandDF.index:
                    gov_name = demandDF.iloc[d,0]
                    for month in range(1,13):
                        self.demand_forecast[(gov_name,month)]=self.demand_forecast[(gov_name,month)]+float(shift_DF.iloc[d,month])

        self.absolute_baseline_surplus = {
            'ajloun': 6238816,
            'amman': 243149540,
            'aqaba': 11291565,
            'balqa': 35374605,
            'irbid': 78565146,
            'jarash': 10485048,
            'karak': 21434408,
            'maan': 11183279,
            'madaba': 15612456,
            'mafraq': 31658232,
            'tafilah': 6448176,
            'zarqa': 80540200}

        """
            WAJ consumer surplus compensation option:
            WAJ can compensate declining consumer surpluses by increasing the allocation target. e.g. a
            e.g. a 100% reactive WAJ will double the delivery target if consumer surplus falls by 50%.
            a 50% reactive WAJ would increase the target by 1.5.
        """

        if timestep.year > 2006:
            haw = self.network.get_institution('human_agent_wrapper')
            for g in haw.govs:
                name = g.name[:-8]
                surplus_diff = sum(g.get_history("total_consumer_surplus_diff")[-12:])
                surplus_diff_prev = sum(g.get_history("total_consumer_surplus_diff")[-24:-12])
                self.gov_surplus_prev_year[(name,month)]=surplus_diff
                base_surplus = self.absolute_baseline_surplus[name]
                abs_surplus = surplus_diff + base_surplus
                abs_surplus_prev=surplus_diff_prev + base_surplus
                sensi = self.network.parameters.waj['consumer_surplus_reactivity']
                for month in range(1, 13):
                    if abs_surplus <  abs_surplus_prev:
                        self.demand_forecast[(name, month)] *= (1 + sensi * max(0,(1 - max(abs_surplus / abs_surplus_prev, 0))))
                    self.consumer_compensation[(name, month)] =(1 + sensi * max(0,(1 - max(abs_surplus / abs_surplus_prev, 0))))

        for month in range(1,13):
            self.total_demand[month]=0
            for d in demandDF.index: 
                gov_name = demandDF.iloc[d,0]
                self.total_demand[month]=self.total_demand[month]+self.demand_forecast[(gov_name,month)]


    def get_segments(self,x0,xmax,ninc):
        """
        Returns the parameters needed for the set of auxiliary linear constraints,
        alloming for a linear approximation of a square function.

        Only applies for the right half of the funtion (x>x0)

        arguments:
        x0: the horizontal shift of x, where x^2 reaches a minimum
        xmax: the upper boundary of the approximation
        ninc: number of intervals (segments) between x0 and xmax.
        The more the closer to a square function and the slower the calculation.

        returns:
        a list of slopes for each sample point
        a dictionnary association to each cost the corresponding Y axis intercept.
        """

        x0=float(x0)
        xmax=float(xmax)
        ninc=float(ninc)
        
        xa=x0
        xb=0
        coefs=[]
        intercepts={}
        
        while xb<xmax:
            xb=xa+(xmax-x0)/ninc
            ya=(xa-x0)**2
            yb=(xb-x0)**2
            
            m=(yb-ya)/(xb-xa)  # slope
            n=ya-m*xa          # intercept     
            
            coefs.append(m)
            intercepts[m]=n
            xa=xb
        return coefs,intercepts
        
        
    def get_penalties(self,inc,n_inc,cost_inc,c0):
        """
        This method returns the paramters needed for the set of auxiliary constraints, 
        alloming for a linear approximation of a quadratic optimisation. 
        Penalties of deficits (resp. benefits from surpluses) per capita 
        are increased (resp. decreased) progressively at a given rate.
        The objective is to make the code distribute deficits and surpluses accross the country the same way a quadratic
          function could work.
        
        inputs:
        inc: the increment in yearly water per capita that defines an interval. e.g. 1 m3/cap
        cost_inc: a multiplier to change the cost each interval. To increase the cost of deficits
          (and decrease the benefit of surpluses) of 10% each interval, enter 0.1.
        n_inc: number of intervals. Beyond that, marginal cost becomes constant.
        c0: the intial cost attributed for the first unit of deficit/surplus
        
        returns:
        a list of different costs
        a dictionnary association to each cost the corresponding Y axis intercept.
        """
        C=[]
        B={}        
        
        x=-n_inc*inc
        c=c0
        b=1-c*x
        n=-n_inc
        
        while n <= n_inc:
            C.append(c)
            B[c]=b
            
            x=x+inc
            c2=c*cost_inc
            b2=c*x-c2*x+b
            
            b=b2
            c=c2
            n=n+1       
  
        return C,B
        
    def get_model(self):
        """
            Get the pyomo abstract model.
            If it's already been created, return it. If not, create it.
        """
        
        if self.model is not None:
            return self.model
        
        for i in self.institutions:
            if i.component_type == 'GOV':
                self.demand_nodes.append(i.name)
                if i.name not in ['tafilah', 'aqaba']: 
                    self.linked_demand_nodes.append(i.name)

        for node in  self.network.nodes:
            if node.component_type != 'Groundwater' and node.component_type !='HighlandFarmAgent' and \
                    node.component_type !='UrbanHHAgent':
                self.nodes_names.append(node.name)
                if node.name not in self.demand_nodes:
                    self.non_demand_nodes.append(node.name)
                
            
        for link in self.network.links :
            if link.linktype=='Pipeline':
                self.links_comp[(link.start_node.name, link.end_node.name)]=link
                self.upper_flow[(link.start_node.name, link.end_node.name)]=link.max_flow
                self.cost[(link.start_node.name, link.end_node.name)]=link.cost

        # Declaring model
        model = AbstractModel()
        model.months = Set(initialize=range(1,13))
        model.nodes = Set(initialize=self.nodes_names)
        model.links = Set(within=model.nodes * model.nodes, initialize=self.links_comp.keys())  
  
        model.demand_nodes = Set(initialize=self.demand_nodes)
        model.linked_demand_nodes=Set(initialize=self.linked_demand_nodes)

        model.non_demand_nodes= Set(initialize=self.non_demand_nodes)
        model.nonstorage_nodes = Set(initialize=self.nonstorage_nodes)
        model.storage_nodes = Set(initialize=self.storage_nodes)

        # Declaring model parameters
        model.cost = Param(model.links, initialize=self.cost, default=0)

        model.GWcost = Param(model.nodes, initialize=self.gov_extraction_costs, default=35)

        model.upper_flow = Param(model.links, initialize=self.upper_flow)
        model.flow_lower_bound = Param(model.links, initialize= self.lower_flow)
        model.storage_lower_bound = Param(model.storage_nodes, initialize=self.min_storage)
        model.storage_upper_bound = Param(model.storage_nodes, initialize=self.max_storage)
        model.inflow = Param(model.nodes, initialize=self.inflow)
        model.consumption_coefficient = Param(model.nodes, initialize=self.consumption_coefficient)
        model.flow_multiplier = Param(model.links, initialize=self.flow_multiplier)   
        
        self.model=model

        return self.model

    # ++++++++++++++++++++++
    #  WAJ: Yearly forecast:
    # ++++++++++++++++++++++

    def set_allocation_forecast(self, timestep):
        """
        Determines water extraction and allocation in each governorate for the next 12 months.
        Current method: minimize total yearly deficits, then minimize transfer costs.

        **Inputs**:
        network properties
        
        **Same agent inputs**:
        |  *demand_forecast*
        |  *yearly_max_groundwater_pumping*   
        |  *extraction_cap*   
        
        **Other agents inputs**:
        |  *CWA: kac_to_zai_forecast

        **Exogenous inputs:


        **Outputs**:
        |  *allocation_forecast*
        |  *pumping_forecast*
        |  *deficit_forecast*
        |  *transfer_forecast*
        """
        
        logging.info("Setting allocation forecast.")
        
        self.allocation_forecast={}
        self.pumping_forecast={}
        self.transfer_forecast={}        
        self.deficit_forecast={}
        self.surplus_forecast={}        
        self.max_pump={}

        if self.network.parameters.waj['passive_WAJ']==1:
            return

        self.linear_method=self.network.parameters.waj['linear_method']
        
        self.test_equity=self.network.parameters.waj['test_equity']

        self.extraction_cap_forecast=self.extraction_cap

        for month in range(1,13):
            self.extraction_cap_forecast[('zai',month)] = \
                self.network.get_institution('cwa').kac_to_zai_forecast[month-1]*(1+self.SWR)
            if timestep.year==2013:
                self.extraction_cap_forecast[('maan',month)]=self.Maan_2013[month]

        model = deepcopy(self.get_model())
        
        # Set marginal penalties for deficits:
        if self.linear_method=='2015':
            marg_costs, B = self.get_penalties(1,3,20,10.0)

            model.marg_costs = Set(initialize=marg_costs)
            model.intercepts = Param(model.marg_costs, initialize=B)
            
        elif self.linear_method=='quad':
            if self.test_equity==0:

                self.Nintervals=500
                self.Nintervals_short=100
                
                xmax=10
                slopes,B = self.get_segments(0,xmax,self.Nintervals)
                self.quad_slopes=slopes
                self.quad_intercepts=B
                
                self.quad_slopes_short=[]
                self.quad_intercepts_short={}
                i=0
                j=0
                for i in range(0,self.Nintervals_short):
                    if i<=40:
                        self.quad_slopes_short.append(self.quad_slopes[i])
                        
                    else:
                        j=j+1
                        if 40+8*j<=500:
                            self.quad_slopes_short.append(self.quad_slopes[40+8*j])
                    if 40+8*j<=500:
                        slope=self.quad_slopes_short[i]
                        self.quad_intercepts_short[slope]=self.quad_intercepts[slope]

                model.Nintervals=Set(initialize=range(0,self.Nintervals))
                
                model.Nintervals_short=Set(initialize=range(0,len(self.quad_intercepts_short)))

        for node in  self.nodes_names:
            self.inflow[(node)]=0
            self.max_pump[(node)]=0            
        for key1 in self.yearly_max_groundwater_pumping.keys():
            self.max_pump[key1]=self.yearly_max_groundwater_pumping[key1]

        model.demand = Param(model.demand_nodes, model.months, initialize=self.demand_forecast)
        model.max_pump = Param(model.nodes, initialize=self.max_pump)
        model.extr_cap = Param(model.nodes, model.months, initialize=self.extraction_cap_forecast, default=0)    

        # +++ Decision Variables +++
        def extr_capacity_constraint(model, node, months):
            return (0, model.extr_cap[node, months])
            
        def deficit_constraint(model, node, months):
            return (-model.demand[node, months]/2, model.demand[node, months])

        model.Q = Var(model.links, model.months, domain=NonNegativeReals)

        model.P = Var(model.nodes, model.months, domain=NonNegativeReals, bounds=extr_capacity_constraint)

        if self.test_equity==1:
            model.inequity = Var(model.demand_nodes, model.months, domain=NonNegativeReals)
            model.deficit = Var(model.demand_nodes, model.months, domain=Reals, bounds=deficit_constraint)
            model.max_def = Var(domain=Reals)
            model.min_def = Var(domain=Reals)
            model.yearlymax=Var(model.demand_nodes,domain=Reals)
        else:
            model.aux_def2 = Var(model.demand_nodes, model.months, domain=NonNegativeReals)
            model.aux_surp2 = Var(model.demand_nodes, model.months, domain=NonNegativeReals)
            model.deficit = Var(model.demand_nodes, model.months, domain=Reals, bounds=deficit_constraint)
            model.pos_deficit = Var(model.demand_nodes, model.months, domain=NonNegativeReals)

        # +++ Objective function +++
        if self.test_equity==1:

            def objective_function(model):
                
                C_spread = 10000
                C_max=C_spread*1000
                C_ineq = 10000
                C_real = 10**-8
                return C_spread*(sum(model.max_pump[node] for node in model.nodes) - sum(model.P[node, month]
                                for node in model.demand_nodes
                                for month in model.months)) + \
                       C_ineq*sum(model.inequity[node, month] for node in model.demand_nodes for month in model.months) \
                       + C_real*sum(model.cost[link]*model.Q[link, month] for month in model.months
                                    for link in model.links) + C_max*model.max_def - C_max*model.min_def

        elif self.network.parameters.waj['def_approach']=='negative_deficit':
             if self.solver_status != 'ok':
                def objective_function(model):
                    Cdef=self.network.parameters.waj['deficit_penalty']
                    Csurplus=self.network.parameters.waj['surplus_bonus']
                    Ctrans=self.network.parameters.waj['transfer_penalty']
                    Cpump=self.network.parameters.waj['extraction_penalty']
                    return Cdef*100 + sum(model.aux_def2[node, month]/model.demand[node,month] * 1000000
                                    for node in model.demand_nodes for month in model.months) * Cdef + \
                           Csurplus*sum(model.aux_surp2[node,month]/model.demand[node,month] * 1000000 -
                                        2 * (model.pos_deficit[node,month]-model.deficit[node,month])/1000000
                                        for node in model.demand_nodes for month in model.months) + \
                           sum(model.cost[link]*model.Q[link, month] for month in model.months for link in model.links)*\
                           Ctrans + sum(model.GWcost[node]*model.P[node,month] for node in model.nodes
                                        for month in model.months)*Cpump
             elif self.network.parameters.waj['solver']=='cplex':
                def objective_function(model):
                    Cdef=self.network.parameters.waj['deficit_penalty']
                    Csurplus=self.network.parameters.waj['surplus_bonus']
                    Ctrans=self.network.parameters.waj['transfer_penalty']
                    Cpump=self.network.parameters.waj['extraction_penalty']
                    return Cdef*100 + sum( (model.pos_deficit[node, month]**2)/model.demand[node,month]/1000000
                                    for node in model.demand_nodes for month in model.months)*Cdef + \
                           Csurplus*sum( ((model.pos_deficit[node,month]-model.deficit[node,month])**2) /
                                         model.demand[node,month]/1000000-
                                         2*(model.pos_deficit[node,month]-model.deficit[node,month])/1000000
                                         for node in model.demand_nodes for month in model.months) + \
                           sum(model.cost[link]*model.Q[link, month] for month in model.months for link in model.links)*\
                           Ctrans + sum(model.GWcost[node]*model.P[node,month] for node in model.nodes
                                        for month in model.months)*Cpump

        model.Z = Objective(rule=objective_function, sense=minimize)

        # +++ Constraints +++
        def flow_capacity(model, node, node2, month):
            return model.Q[node,node2,month]<=model.upper_flow[node, node2]

        model.flow_capacity_const=Constraint(model.links, model.months, rule = flow_capacity)

        # +++ Equity constraints: +++
        if self.test_equity==1:
            
            def equity_plus(model,node,month):
                return model.deficit[node,month]/model.demand[node,month] - \
                       (sum(model.deficit[node2,month2]/model.demand[node2,month2] for node2 in model.demand_nodes
                            for month2 in model.months))/144 <= model.inequity[node,month]
            model.equity_plus_const=Constraint(model.demand_nodes, model.months, rule=equity_plus)
            
            def equity_neg(model,node,month):
                return - model.deficit[node,month]/model.demand[node,month] + \
                       (sum(model.deficit[node2,month2]/model.demand[node2,month2] for node2 in model.demand_nodes
                            for month2 in model.months))/144 <= model.inequity[node,month]
            model.equity_neg_const=Constraint(model.demand_nodes, model.months, rule=equity_neg)
            
        else:
            if self.linear_method=='2015':
                def marg_deficit(model,node,month,marg_cost):
                    return marg_cost*model.deficit[node, month]/self.population[node] + \
                           model.intercepts[marg_cost] <= model.aux_cost[node, month]
                model.marg_deficit_const = Constraint(model.demand_nodes, model.months, model.marg_costs,
                                                      rule=marg_deficit)
                   
            elif self.linear_method=='quad':

                def positive_deficit(model,node,month):
                    return model.deficit[node,month]<=model.pos_deficit[node,month]
                model.pos_def_const = Constraint(model.demand_nodes, model.months, rule=positive_deficit)
                
                if self.solver_status!='ok':
                    def linear_def_costs(model,node,month,interval):
                        slope=self.quad_slopes_short[interval]  # returns a N_intervals array   
                        intercept=self.quad_intercepts_short[slope]
    
                        return slope*model.pos_deficit[node, month]/1000000 + intercept <= model.aux_def2[node, month]                            
                    model.linear_def_const = Constraint(model.demand_nodes, model.months, model.Nintervals_short,
                                                        rule=linear_def_costs)
    
                    def linear_surplus_costs(model,node,month,interval):
                        slope=self.quad_slopes_short[interval]  # returns a N_intervals array   
                        intercept=self.quad_intercepts_short[slope]
                        return slope*(model.pos_deficit[node, month]-model.deficit[node,month])/1000000 + intercept <= \
                               model.aux_surp2[node, month]
                    model.linear_surplus_const = Constraint(model.demand_nodes, model.months, model.Nintervals_short,
                                                            rule=linear_surplus_costs)

        model.incoming_links = None
        model.outgoing_links = None

        def mass_balance_demand(model, node, month):

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in  = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            # imports
            term1 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_in])
            # extraction                     
            term2 = model.P[node, month]
            # deficit
            term3 = model.deficit[node, month]
            #  exports  
            term4 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_out])
           
            # demand           
            term5 = model.demand[node, month]

            return (term1 + term2 + term3) - (term4 + term5) == 0
            
        model.mass_balance_demand_const = Constraint(model.demand_nodes, model.months, rule=mass_balance_demand)
        
        def mass_balance(model, node, month):

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in  = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            # imports
            term1 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_in])
            # extraction                     
            term2 = model.P[node, month]

            #  exports  
            term4 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_out])
         
            return term1 + term2 - term4 == 0
            
        model.mass_balance_const = Constraint(model.non_demand_nodes, model.months, rule=mass_balance)

        if self.yearly_GW_max_scale=='gov':
            
            def extraction_limit(model, nodes, months):
                yearly_P = sum(model.P[nodes, month]
                                for month in model.months)
                return yearly_P <= model.max_pump[nodes]
            model.extraction_limit_const = Constraint(model.nodes, model.months, rule = extraction_limit)
            
        elif self.yearly_GW_max_scale=='country':
            
            def extraction_limit(model):
                total_P = sum(model.P[node, month]
                                for month in model.months
                                for node in model.nodes)


                return total_P <= sum(model.max_pump[node] for node in model.nodes)
                
            model.extraction_limit_const = Constraint(rule = extraction_limit)
            
            
        def dilution(model, month):  
            """
            the Disi dilution constraint, can be genralized later on for any dilution, e.g. saline sources
            IMPORTANT
            assumptions:
            - Disi waters are diluted at equal parts with other sources
            - Implemented for Amman, should be done for Karak and Madaba once reverse flows are allowed
            """
            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            node='amman'
            nodes_in  = model.incoming_links.get(node, [])

            term1 = sum([model.Q[n1, n2, month] for n1, n2 in nodes_in])+model.P[node,month]

            term2 = model.Q['maan','karak_jnc', month]
            
            dilution = self.network.parameters.waj['dilution']
            
            return term1>=dilution*term2
            
        model.dilution_const = Constraint(model.months, rule=dilution)

        def smooth_transfer_sup(model, node, node2, month):
        
            if month < 12:
                month2 = month +1    
            else: 
                month2 = 1
            term1 = model.Q[node, node2, month] 
            
            term2 = model.Q[node, node2, month2]

            if timestep.year==2013:
                return term1 >=0

            else:
                return term2 <= 1.25*term1

        model.smooth_transfer_up_const = Constraint(model.links, model.months, rule = smooth_transfer_sup)    
        
        def smooth_transfer_low(model, node, node2, month):

            if month < 12:
                month2 = month +1    
            else: 
                month2 = 1
            term1 = model.Q[node, node2, month] 
            
            term2 = model.Q[node, node2, month2]
            
            if timestep.year==2013:
                return term1 >=0

            else:
                return term2 >= 0.75*term1
        
        model.smooth_transfer_low_const = Constraint(model.links, model.months, rule = smooth_transfer_low)    
        
        def smooth_extraction_sup(model, nodes, month):
        
            if month < 12:
                month2 = month +1    
            else: 
                month2 = 1               
            term1 = model.P[nodes, month]             
            term2 = model.P[nodes, month2]
            if nodes == 'amman':
                return term2 <= 1.1*term1
            elif nodes == 'maan' and timestep.year==2013:
                return term1 >= 0
            else:
                return term2 <= 1.3*term1
        
        model.smooth_extraction_sup_const = Constraint(model.nodes, model.months, rule = smooth_extraction_sup)    
        
        def smooth_extraction_low(model, nodes, month):
         
            if month < 12:
                month2 = month +1    
            else: 
                month2 = 1
            term1 = model.P[nodes, month] 
            term2 = model.P[nodes, month2]  
            if nodes == 'amman':
                return term2 >= 0.9*term1
            elif nodes == 'maan' and timestep.year==2013:
                return term1 >= 0
            else:
                return term2 >= 0.7*term1
        
        model.smooth_extraction_low_const = Constraint(model.nodes, model.months, rule = smooth_extraction_low)    

        # +++ SOLVER: +++
        if self.solver_status == 'ok':
            opt = SolverFactory("ipopt", solver_io='nl')
            instance = model.create_instance()
            instance.dual = Suffix(direction=Suffix.IMPORT)
            try:
                results = opt.solve(instance)
                model.solutions.load_from(results)
                # logging.info(results)
                solver_name = "ipopt"
            except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                self.solver_status = "ipopt_failed"

            if self.solver_status != "ipopt_failed":
                if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition ==
                                                                   TerminationCondition.optimal):
                    pass
                else:
                    logging.info("waj failed to solve using ipopt ... retrying")
                    opt = SolverFactory("cplex")
                    instance = model.create_instance()
                    instance.dual = Suffix(direction=Suffix.IMPORT)
                    try:
                        results = opt.solve(instance)
                        model.solutions.load_from(results)
                        # logging.info(results)
                        solver_name = "cplex"
                    except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                        self.solver_status = "cplex_failed"
                        logging.info("waj failed to solve using cplex")
            else:
                logging.info("waj failed to solve using ipopt ... retrying")
                opt = SolverFactory("cplex")
                instance = model.create_instance()
                instance.dual = Suffix(direction=Suffix.IMPORT)
                try:
                    results = opt.solve(instance)
                    model.solutions.load_from(results)
                    # logging.info(results)
                    solver_name = "cplex"
                except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                    self.solver_status = "cplex_failed"
                    logging.info("waj failed to solve using cplex")

            if self.solver_status != "cplex_failed":
                if (results.solver.status == SolverStatus.ok) and (
                        results.solver.termination_condition == TerminationCondition.optimal):
                    logging.info("waj solved using " + solver_name)
                else:
                    self.solver_status = "not_optimal"
                    logging.info("waj failed to solve using cplex")
            else:
                self.solver_status = "not_optimal"
        else:
            opt = SolverFactory("glpk")
            instance = model.create_instance()
            instance.dual = Suffix(direction=Suffix.IMPORT)
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            logging.info("waj solved using glpk")

        for var in model.component_objects(Var, active=True):
            var=str(var)
            if var=="P":
                p_var = getattr(instance, var)
                total=0
                for vv in p_var:
                    name= ''.join(map(str,vv))
                    total=total+p_var[vv].value
                # print "total %s : "%p_var, total
            elif var=="deficit":
                d_var = getattr(instance, var)
                total=0
                for vv in d_var:
                    name= ''.join(map(str,vv))
                    total=total+d_var[vv].value
                # print "total %s : "%d_var, total
            elif var=="Q":
                q_var = getattr(instance, var)
                total=0
                for vv in q_var:
                    name= "(" + ', '.join(map(str,vv)) + ")"
                    total=total+q_var[vv].value
                # print "total %s : "%q_var, total                   

        for i in self.network.institutions:
            if i.component_type == 'GOV':

                for var in model.component_objects(Var, active=1):
                    var=str(var)
                    if (var=="deficit"):
                        d_var = getattr(instance, var)
                        for month in range(1,13):
                            self.deficit_forecast[(i.name,month)] = d_var[(i.name,month)].value

                    elif (var=="P"):
                        p_var = getattr(instance, var)
                        for month in range(1,13):
                            self.pumping_forecast[(i.name,month)] = p_var[(i.name,month)].value
                    elif (var=="surplus"):
                        p_var = getattr(instance, var)
                        for month in range(1,13):
                            self.surplus_forecast[(i.name,month)] = p_var[(i.name,month)].value

                for month in range(1,13):            
                    self.allocation_forecast[(i.name,month)]=self.demand_forecast[(i.name,month)] - \
                                                             self.deficit_forecast[(i.name,month)]

        for var in model.component_objects(Var, active=1):     
            var=str(var)

            if (var=="P"):
                 p_var = getattr(instance, var)
                 for name in self.yearly_max_groundwater_pumping.keys():
                     for month in range(1,13):
                        self.pumping_forecast[(name,month)] = p_var[(name,month)].value

        for var in model.component_objects(Var, active=1):
            var=str(var)
            if (var=="Q"):
               q_var = getattr(instance, var)
               
               numT=0
               for line2 in q_var:
                   numT+=1
               numT=int(numT/12)    
               
               if self.export_data==1:
                   transfers= pd.DataFrame(index=range(0,numT), columns=range(0,14))
    
               for month in range(1,13):
                   l=0
                   for line in q_var: 
                        start_node=line[0]
                        end_node=line[1]
                        monthq = line[2]
                        Q_value=q_var[line].value
                        
                        self.transfer_forecast[line]=Q_value
                        
                        if month==monthq:
                            if self.export_data==1:
                                transfers.iloc[l,0]=start_node
                                transfers.iloc[l,1]=end_node
                                transfers.iloc[l,1+month]=Q_value
                            self.transfer_forecast[line]=Q_value
                            l=l+1
        if self.export_data==1:
            if self.WAJ_inputs is None:
                self.WAJ_inputs = get_excel_data("WAJ_inputs.xlsx")
            pumpDF=self.WAJ_inputs.parse('tot_extraction2006')
            demandDF=self.WAJ_inputs.parse('real_supply2006')[0:12]

            if self.writer is None:
                waj_forecasts = os.path.join(basepath, 'data', 'excel_data', 'WAJ_forecasts.xlsx')
                self.writer = pd.ExcelWriter(waj_forecasts, engine='xlsxwriter')                    
            
            # +++ initialize matrix size: +++
            DF_allocation_forecast= pd.DataFrame(index=demandDF.index, columns=demandDF.columns)  
            DF_pumping_forecast = pd.DataFrame(index=pumpDF.index, columns=demandDF.columns)
            DF_deficit_forecast= pd.DataFrame(index=demandDF.index, columns=demandDF.columns)

            d=0
            for d in demandDF.index:
                gov_name = demandDF.iloc[d,0]
                DF_allocation_forecast.iloc[d,0]=gov_name
                DF_deficit_forecast.iloc[d,0]=gov_name
                for month in range(1,13):
                    DF_allocation_forecast.iloc[d,month]=self.allocation_forecast[(gov_name,month)]
                    DF_deficit_forecast.iloc[d,month]=self.deficit_forecast[(gov_name,month)]

            p=0
            for p in pumpDF.index:
                node_name = pumpDF.iloc[p,0]
                DF_pumping_forecast.iloc[p,0]=node_name
                for month in range(1,13):
                    DF_pumping_forecast.iloc[p,month]=self.pumping_forecast[(node_name,month)]

            # Get duals:
            Bconst = getattr(instance, 'mass_balance_demand_const')     
                
            DF_balance_duals= pd.DataFrame(index=demandDF.index, columns=demandDF.columns)           
            d=0
            for d in demandDF.index:
                gov_name = demandDF.iloc[d,0]
                DF_balance_duals.iloc[d,0]=gov_name
                for month in range(1,13):
                    DF_balance_duals.iloc[d,month]=instance.dual.get(instance.mass_balance_demand_const[gov_name,month])

            DF_flow_duals= pd.DataFrame(index=transfers.index, columns=transfers.columns)           
            d=0
            for d in transfers.index:
                node_name1 = transfers.iloc[d,0]
                node_name2 = transfers.iloc[d,1]
                DF_flow_duals.iloc[d,0]=node_name1
                DF_flow_duals.iloc[d,1]=node_name2
                for month in range(1,13):
                    DF_flow_duals.iloc[d,month+1]=instance.dual.get(instance.flow_capacity_const[node_name1,node_name2,month])

            if timestep.year>self.startyear:
                colnum=1+12*(timestep.year-self.startyear)
                colnumT=colnum+1
                DF_allocation_forecast.drop('name',1,inplace=True)
                DF_pumping_forecast.drop('name',1,inplace=True)
                DF_deficit_forecast.drop('name',1,inplace=True)
                DF_balance_duals.drop('name',1,inplace=True)
                transfers.drop(transfers.columns[[0,1]],1,inplace=True)
                DF_flow_duals.drop(transfers.columns[[0,1]],1,inplace=True)
            else:
                colnum=0
                colnumT=0
            
            DF_allocation_forecast.to_excel(self.writer, sheet_name='supply',index=False, startcol=colnum)        
            DF_deficit_forecast.to_excel(self.writer, sheet_name='deficit',index=False, startcol=colnum)        
            DF_pumping_forecast.to_excel(self.writer, sheet_name='extraction',index=False, startcol=colnum)
            transfers.to_excel(self.writer, sheet_name='transfers',index=False, startcol=colnumT) 
            DF_balance_duals.to_excel(self.writer, sheet_name='balance_duals',index=False, startcol=colnum)
            DF_flow_duals.to_excel(self.writer, sheet_name='transfer_duals',index=False, startcol=colnumT)

            self.endyear=self.startyear+self.numyears-1
            if timestep.year==self.endyear:
                self.writer.save()
            
            logging.info('WAJ annual allocation done.')


    # ++++++++++++++++++++++
    # WAJ: MONTHLY DELIVERY:
    # ++++++++++++++++++++++

    def determine_gov_delivery(self, timestep):
        """
        WAJ decides real allocation based on forecasts. Currently no difference. 
        Implementing: re-compute forecast if a threshold is crossed in demand or supply (e.g. 20% change)
        
        """
        
        if self.network.parameters.waj['passive_WAJ']==1:
            if self.WAJ_inputs is None:
                self.WAJ_inputs = get_excel_data("WAJ_inputs.xlsx")
            if timestep.year>2014:
                supplyDF=self.WAJ_inputs.parse('real_supply2014')[0:12]    
                extractionDF=self.WAJ_inputs.parse('monthly_extraction2014')
            elif timestep.year<2006:
                supplyDF=self.WAJ_inputs.parse('real_supply2006')[0:12]    
                extractionDF=self.WAJ_inputs.parse('monthly_extraction2006')
            else:
                supplyDF=self.WAJ_inputs.parse('real_supply'+str(timestep.year))[0:12]    
                extractionDF=self.WAJ_inputs.parse('monthly_extraction'+str(timestep.year))
            
            for d in supplyDF.index: 
                gov_name = supplyDF.iloc[d,0]
                self.gov_delivery[gov_name]=supplyDF.iloc[d,timestep.month]
                
                if timestep.year>2014:
                    self.gov_delivery[gov_name]=supplyDF.iloc[d,timestep.month]*((1+self.pop_growth)**(timestep.year-2014))
                if timestep.year<2006:
                    self.gov_delivery[gov_name]=supplyDF.iloc[d,timestep.month]*((1-self.pop_growth)**(2006-timestep.year))
            for d in extractionDF.index: 
                node_name = extractionDF.iloc[d,0]            
                self.real_extraction[node_name]=extractionDF.iloc[d,timestep.month]
                
                if timestep.year>2014:
                    self.real_extraction[node_name]=extractionDF.iloc[d,timestep.month]*((1+self.pop_growth)**(timestep.year-2014))
                if timestep.year<2006:
                    self.real_extraction[node_name]=extractionDF.iloc[d,timestep.month]*((1-self.pop_growth)**(2006-timestep.year))
            
            return

        self.extr_MF={}  # MF = Monthly Forecast
        self.allocation_MF={}
        self.transfer_MF = {}
        self.actual_extraction_cap = {}

        model = deepcopy(self.get_model())          
        
        # +++ 1. Update data +++
        for node in self.nodes_names:
            self.inflow[(node)]=0
            self.extr_MF[node]=0  

        self.actualize_wells_share(timestep)
        for name in self.yearly_max_groundwater_pumping.keys():           
            self.extr_MF[name] = self.pumping_forecast[(name, timestep.month)]

            if name in self.gov_list:
                if self.gov_missing_ratio[name]==1:
                    self.extraction_cap[(name,timestep.month)]=self.extraction_cap[(name,timestep.month)]*\
                                                               (1-self.gov_missing_ratio[name])

            self.actual_extraction_cap[name] = self.extraction_cap[(name, timestep.month)]

            if name in self.demand_nodes:   
                self.allocation_MF[name] = self.allocation_forecast[(name, timestep.month)]

        self.actual_extraction_cap['zai'] = self.network.get_institution('cwa').kac_zai_actual

        for (n1,n2) in self.links_comp.keys():
            self.transfer_MF[(n1, n2)] = self.transfer_forecast[(n1, n2, timestep.month)]

        # +++ Check dry: +++

        # +++ 2. Re-run reduced version of the math program +++

        model.extr_cap = Param(model.nodes, initialize=self.actual_extraction_cap, default=0)    
        
        model.allocation_F = Param(model.demand_nodes, initialize=self.allocation_MF)
        model.extraction_F = Param(model.nodes, initialize=self.extr_MF, default=0)
        model.transfer_F = Param(model.links, initialize=self.transfer_MF)

        # +++ Decision Variables +++

        # +++ Defining the flow upper bound +++
        def flow_capacity_constraint(model, node, node2):
            return (0, model.upper_flow[node, node2])

        def extr_capacity_constraint(model, node):
            return (0, model.extr_cap[node])

        model.Q = Var(model.links, domain=NonNegativeReals, bounds=flow_capacity_constraint)            
        model.P = Var(model.nodes, domain=NonNegativeReals, bounds=extr_capacity_constraint)        
        model.supply = Var(model.demand_nodes, domain=NonNegativeReals)    
        
        # Auxiliaries: absolute value equivalent
        model.Q_spread = Var(model.links, domain=NonNegativeReals)
        model.P_spread = Var(model.nodes, domain=NonNegativeReals)
        model.supply_spread = Var(model.demand_nodes, domain=NonNegativeReals)  

        # +++ Objective function +++
        def objective_function(model):
            C_month_supply=self.network.parameters.waj['C_month_supply']
            C_month_transfer=self.network.parameters.waj['C_month_transfer']
            C_month_extr=self.network.parameters.waj['C_month_extr']
            return sum(C_month_supply*model.supply_spread[node] for node in model.demand_nodes) + \
                   sum(C_month_transfer*model.Q_spread[link] for link in model.links) + \
                   sum(C_month_extr*model.P_spread[node] for node in model.nodes) + \
                   sum(model.cost[link]*model.Q[link] for link in model.links)
        model.Z = Objective(rule=objective_function, sense=minimize)

        # +++ Constraints +++
        # 1. auxiliary constraints:
        def Q_absolute_plus(model,node,node2):
            return model.Q[node,node2]-model.transfer_F[node,node2] <= model.Q_spread[node,node2]                              
        model.Q_absolute_plus_const = Constraint(model.links, rule=Q_absolute_plus)

        def Q_absolute_neg(model,node,node2):
            return - model.Q[node,node2] + model.transfer_F[node,node2] <= model.Q_spread[node,node2]                              
        model.Q_absolute_neg_const = Constraint(model.links, rule=Q_absolute_neg)        

        def P_absolute_plus(model,node):
            return model.P[node] - model.extraction_F[node] <= model.P_spread[node]                                
        model.P_absolute_plus_const = Constraint(model.nodes, rule=P_absolute_plus)
        
        def P_absolute_neg(model,node):
            return - model.P[node] + model.extraction_F[node] <= model.P_spread[node]                                
        model.P_absolute_neg_const = Constraint(model.nodes, rule=P_absolute_neg)
        
        def S_absolute_plus(model,node):
            return model.supply[node] - model.allocation_F[node] <= model.supply_spread[node]                                
        model.S_absolute_plus_const = Constraint(model.demand_nodes, rule=S_absolute_plus)
        
        def S_absolute_neg(model,node):
            return - model.supply[node] + model.allocation_F[node] <= model.supply_spread[node]                                
        model.S_absolute_neg_const = Constraint(model.demand_nodes, rule=S_absolute_neg)

        # 2. physical constraints
        model.incoming_links = None
        model.outgoing_links = None

        def m_mass_balance_demand(model, node):
            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in  = model.incoming_links.get(node, [])
            nodes_out = model.outgoing_links.get(node, [])

            # imports
            term1 = sum([model.Q[n1, n2] for n1, n2 in nodes_in])
            # extraction                     
            term2 = model.P[node]

            #  exports  
            term4 = sum([model.Q[n1, n2] for n1, n2 in nodes_out])
            # demand           
            term5 = model.supply[node]

            return (term1 + term2) - (term4 + term5) == 0
            
        model.m_mass_balance_demand_const = Constraint(model.demand_nodes, rule=m_mass_balance_demand)
        
        def m_mass_balance(model, node):

            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            nodes_in2  = model.incoming_links.get(node, [])
            nodes_out2 = model.outgoing_links.get(node, [])

            # imports
            term1 = sum([model.Q[n1, n2] for n1, n2 in nodes_in2])
            # extraction                     
            term2 = model.P[node]

            #  exports  
            term4 = sum([model.Q[n1, n2] for n1, n2 in nodes_out2])
         
            return term1 + term2 - term4 == 0
            
        model.m_mass_balance_const = Constraint(model.non_demand_nodes, rule=m_mass_balance)

        def m_dilution(model):  
            """
            the Disi dilution constraint, can be genralized later on for any dilution, e.g. saline sources
            IMPORTANT
            assumptions:
            - Disi waters are diluted at equal parts with other sources
            - Implemented for Amman, should be done for Karak and Madaba once reverse flows are allowed
            """
            if model.incoming_links is None:
                incoming_links = {}
                outgoing_links = {}
                for l in model.links:
                    out_node = l[0]
                    in_node  = l[1]
                    if incoming_links.get(in_node):
                        incoming_links[in_node].append(l)
                    else:
                        incoming_links[in_node] = [l]

                    if outgoing_links.get(out_node):
                        outgoing_links[out_node].append(l)
                    else:
                        outgoing_links[out_node] = [l]

                model.incoming_links = incoming_links
                model.outgoing_links = outgoing_links

            node='amman'
            nodes_in  = model.incoming_links.get(node, [])

            term1 = sum([model.Q[n1, n2] for n1, n2 in nodes_in])+model.P[node]
            term2 = model.Q['maan','karak_jnc']
            
            dilution=self.network.parameters.waj['dilution']
            return term1>=dilution*term2
            
        model.m_dilution_const = Constraint(rule=m_dilution)

        # +++++++++++++++
        # +++ SOLVER: +++
        # +++++++++++++++
        opt = SolverFactory("glpk")
        instance = model.create_instance()
        try:
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "glpk"
            solver_status = "ok"
        except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
            solver_status = "glpk_failed"
        if solver_status != "glpk_failed":
            if (results.solver.status == SolverStatus.ok) and (
                        results.solver.termination_condition == TerminationCondition.optimal):
                pass
            else:
                logging.info(" ... retrying")
                opt = SolverFactory("cplex")
                instance = model.create_instance()
                instance.dual = Suffix(direction=Suffix.IMPORT)
                results = opt.solve(instance)
                model.solutions.load_from(results)
                # logging.info(results)
                solver_name = "cplex"
        else:
            logging.info(" ... retrying")
            opt = SolverFactory("cplex")
            instance = model.create_instance()
            instance.dual = Suffix(direction=Suffix.IMPORT)
            results = opt.solve(instance)
            model.solutions.load_from(results)
            # logging.info(results)
            solver_name = "cplex"
        if (results.solver.status == SolverStatus.ok) and (
                    results.solver.termination_condition == TerminationCondition.optimal):
            logging.info("waj deliver solved using " + solver_name)
        else:
            logging.info("waj delivery failed to solve")

        # ++++++++++++++++
        # +++ Outputs: +++
        # ++++++++++++++++
        for var in model.component_objects(Var, active=1):
            var=str(var)
            if (var=="supply"):
                d_var = getattr(instance, var)
                for name in self.allocation_MF.keys():
                    self.gov_delivery[name] = d_var[name].value

            if (var=="P"):
                 p_var = getattr(instance, var)
                 for name in self.extr_MF.keys():
                     self.real_extraction[name] = p_var[name].value

            if (var=="Q"):
                q_var = getattr(instance, var)
                numT=0
                for line2 in q_var:
                    numT+=1
                if self.export_data==1:
                    real_transfers= pd.DataFrame(index=range(0,numT), columns=range(0,3))

                l=0
                for line in q_var: 
                    start_node=line[0]
                    end_node=line[1]
                    Q_value=q_var[line].value
                    if self.export_data==1:
                        self.real_transfers[line]=Q_value
                        real_transfers.iloc[l,0]=start_node
                        real_transfers.iloc[l,1]=end_node
                        real_transfers.iloc[l,2]=Q_value
                    l=l+1

        if self.export_data==1:
            if self.WAJ_inputs is None:
                self.WAJ_inputs = get_excel_data("WAJ_inputs.xlsx")

            pumpDF=self.WAJ_inputs.parse('tot_extraction2011')[0:14]
            demandDF=self.WAJ_inputs.parse('real_supply2011')[0:12]

            if self.writer2 is None:
                waj_results = os.path.join(basepath, 'data', 'excel_data', 'WAJresults.xlsx')
                self.writer2 = pd.ExcelWriter(waj_results, engine='xlsxwriter')

            # Initialize matrix size:
            DF_gov_delivery= pd.DataFrame(index=demandDF.index, columns=['name',timestep.month])
            DF_real_extraction = pd.DataFrame(index=pumpDF.index, columns=['name',timestep.month])

            d=0
            for d in demandDF.index:
                gov_name = demandDF.iloc[d,0]
                DF_gov_delivery.iloc[d,0]=gov_name
                DF_gov_delivery.iloc[d,1]=self.gov_delivery[gov_name]

            p=0
            for p in pumpDF.index:
                node_name = pumpDF.iloc[p,0]
                DF_real_extraction.iloc[p,0]=node_name
                DF_real_extraction.iloc[p,1]=self.real_extraction[node_name]

            if timestep.year==self.startyear and timestep.month == 1:
                colnum=0
                colnumT=0
            else:
                colnum=12*(timestep.year-self.startyear)+ timestep.month
                colnumT=colnum+1
                DF_gov_delivery.drop('name',1,inplace=True)
                DF_real_extraction.drop('name',1,inplace=True)
                real_transfers.drop(real_transfers.columns[[0,1]],1,inplace=True)

            DF_gov_delivery.to_excel(self.writer2, sheet_name='supply',index=False, startcol=colnum)
            DF_real_extraction.to_excel(self.writer2, sheet_name='extraction',index=False, startcol=colnum)
            real_transfers.to_excel(self.writer2, sheet_name='transfers',index=False, startcol=colnumT)

            self.endyear=self.startyear+self.numyears-1
            if timestep.year==self.endyear and timestep.month==12:
                self.writer2.save()


    def determine_lifeline_supply(self, timestep):
        hh_min = self.network.parameters.waj['waj_lifeline_hh_min']
        co_min = self.network.parameters.waj['waj_lifeline_co_min']
        haw = self.network.get_institution('human_agent_wrapper')
        self.total_lifeline = 0
        for hh in haw.hh_agents:
            hh.lifeline_consumption = 0
            total = (hh.piped_consumption + hh.tanker_consumption) * 1000 / hh.params['hnum']
            if (total < hh_min) and (hh.represented_units > 0):
                hh.lifeline_consumption = ((hh_min - total) * hh.params['hnum'])/1000
                hh.piped_consumption += hh.lifeline_consumption
                self.total_lifeline += hh.lifeline_consumption * hh.represented_units
            else:
                hh.lifeline_consumption = 0
        for hh in haw.rf_agents:
            hh.lifeline_consumption = 0
            total = (hh.piped_consumption + hh.tanker_consumption) * 1000 / hh.params['hnum']
            if (total < hh_min) and (hh.represented_units > 0):
                hh.lifeline_consumption = ((hh_min - total) * hh.params['hnum'])/1000
                hh.piped_consumption += hh.lifeline_consumption
                self.total_lifeline += hh.lifeline_consumption * hh.represented_units
            else:
                hh.lifeline_consumption = 0
        for co in haw.co_agents:
            co.lifeline_consumption = 0
            total = (co.piped_consumption + co.tanker_consumption) * 1000 / co.params[7]
            if (total < co_min) and (co.represented_units > 0):
                co.lifeline_consumption = ((co_min - total) * co.params[7])/1000
                co.piped_consumption += co.lifeline_consumption
                self.total_lifeline += co.lifeline_consumption * co.represented_units
            else:
                co.lifeline_consumption = 0

        # Adds lifeline supply to groundwater pumping in proportion to existing pumping
        pumping_sum = 0
        for g in self.nodes:
            if g.component_type == 'Groundwater':
                pumping_sum += g.pumping
        for g in self.nodes:
            if g.component_type == 'Groundwater':
                g.pumping += (self.total_lifeline/24/60/60) * (g.pumping/pumping_sum)

    def determine_groundwater_pumping(self, timestep):
        """
        Disaggregates governorate extraction into GW nodes based on 2009 ratios
        """

        for g in self.nodes:  # set pumping for each of WAJ groundwater node
            if g.component_type == 'Groundwater':
                for i in self.network.institutions:
                    if i.component_type == 'GOV':
                        if g._gw_node_properties['gov'] == i.name:
                            g.pumping = self.real_extraction[i.name]*self.wells_current_ratio[g.name] / \
                                        (self.monthdays[timestep.month-1]*24*60*60)

        pumping_sum = 0
        for g in self.nodes:
            if g.component_type == 'Groundwater':
                if g._gw_node_properties['gov'] == 'maan':
                    pumping_sum += g.pumping

        if pumping_sum > (1301213. / (30. * 24. * 60. * 60.)):
            disi_pumping = pumping_sum - (1301213. / (30. * 24. * 60. * 60.))
            for g in self.nodes:
                if g.component_type == 'Groundwater':
                    if g._gw_node_properties['gov'] == 'maan':
                        g.pumping -= disi_pumping * self.wells_current_ratio[g.name]
            self.network.get_node('330103_urb_12').pumping += disi_pumping / 2
            self.network.get_node('340202_urb_12').pumping += disi_pumping / 2

        # +++ Re-allocate pumping from 340201_urb_12 +++
        diff = self.network.get_node('340201_urb_12').pumping - \
               self.network.get_node('340201_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('340201_urb_12').pumping = \
            self.network.get_node('340201_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('330103_urb_12').pumping += diff / 2
        self.network.get_node('340202_urb_12').pumping += diff / 2

        # +++ Allocate groundwater pumping for future projects +++
        # sheediya
        sheediya_pumping = self.real_extraction['sheediya'] / (30*24*60*60)
        sum_pumping = 0
        for g in self.network.get_institution('all_gw_nodes').nodes:
            if g.name[-6:] == 'urb_12' and g.name != '340201_urb_12':
                sum_pumping += g._gw_node_properties['baseline_pumping']
        for g in self.network.get_institution('all_gw_nodes').nodes:
            if g.name[-6:] == 'urb_12' and g.name != '340201_urb_12':
                percent = g._gw_node_properties['baseline_pumping'] / sum_pumping
                g.pumping += sheediya_pumping * percent

        # hisban
        hisban_pumping = self.real_extraction['hisban'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('120201_urb_01')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('120201_urb_07')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('120201_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('120201_urb_01').pumping += \
            (self.network.get_node('120201_urb_01')._gw_node_properties['baseline_pumping'] / sum_pumping) * hisban_pumping
        self.network.get_node('120201_urb_07').pumping += \
            (self.network.get_node('120201_urb_07')._gw_node_properties['baseline_pumping'] / sum_pumping) * hisban_pumping
        self.network.get_node('120201_urb_12').pumping += \
            (self.network.get_node('120201_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * hisban_pumping

        # disiamm
        disiamm_pumping = self.real_extraction['hisban'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('330103_urb_12').pumping += \
            (self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * disiamm_pumping
        self.network.get_node('340202_urb_12').pumping += \
            (self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * disiamm_pumping

        # aqeb1
        aqeb_pumping = (self.real_extraction['aqeb1'] + self.real_extraction['aqeb2']) / (30*24*60*60)
        self.network.get_node('220203_urb_02').pumping += aqeb_pumping

        # heedan
        heedan_pumping = self.real_extraction['heedan'] / (30*24*60*60)
        self.network.get_node('140203_urb_07').pumping += heedan_pumping

        # aqagw
        aqagw_pumping = self.real_extraction['aqagw'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('340101_urb_01')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('340102_urb_01')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('340101_urb_01').pumping += \
            (self.network.get_node('340101_urb_01')._gw_node_properties['baseline_pumping'] / sum_pumping) * aqagw_pumping
        self.network.get_node('340102_urb_01').pumping += \
            (self.network.get_node('340102_urb_01')._gw_node_properties['baseline_pumping'] / sum_pumping) * aqagw_pumping
        self.network.get_node('340202_urb_12').pumping += \
            (self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * aqagw_pumping

        # kargw
        kargw_pumping = self.real_extraction['kargw'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('310401_urb_01')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('310402_urb_01')._gw_node_properties['baseline_pumping']
        self.network.get_node('310401_urb_01').pumping += \
            (self.network.get_node('310401_urb_01')._gw_node_properties['baseline_pumping'] / sum_pumping) * kargw_pumping
        self.network.get_node('310402_urb_01').pumping += \
            (self.network.get_node('310402_urb_01')._gw_node_properties['baseline_pumping'] / sum_pumping) * kargw_pumping

        # maagw
        maagw_pumping = self.real_extraction['maagw'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('330101_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('330102_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('330102_urb_12')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('330105_urb_02')._gw_node_properties['baseline_pumping']
        self.network.get_node('330101_urb_02').pumping += \
            (self.network.get_node('330101_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.87/.89)
        self.network.get_node('330102_urb_02').pumping += \
            (self.network.get_node('330102_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.87/.89)
        self.network.get_node('330102_urb_12').pumping += \
            (self.network.get_node('330102_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.87/.89)
        self.network.get_node('330105_urb_02').pumping += \
            (self.network.get_node('330105_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.87/.89)

        sum_pumping = 0
        sum_pumping += self.network.get_node('330103_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('330101_urb_02').pumping += \
            (self.network.get_node('330103_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.02/.89)
        self.network.get_node('330102_urb_02').pumping += \
            (self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * maagw_pumping * (.02/.89)

        # mafgw
        mafgw_pumping = self.real_extraction['mafgw'] / (30*24*60*60)
        self.network.get_node('220401_urb_01').pumping += mafgw_pumping

        # tafgw
        tafgw_pumping = self.real_extraction['tafgw'] / (30*24*60*60)
        self.network.get_node('320301_urb_02').pumping += tafgw_pumping

        # disitaf
        disitaf_pumping = self.real_extraction['disitaf'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping']
        self.network.get_node('330103_urb_12').pumping += \
            (self.network.get_node('330103_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * disitaf_pumping
        self.network.get_node('340202_urb_12').pumping += \
            (self.network.get_node('340202_urb_12')._gw_node_properties['baseline_pumping'] / sum_pumping) * disitaf_pumping

        # hasagw
        hasagw_pumping = self.real_extraction['hasagw'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('320101_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('320101_urb_03')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('320301_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('320301_urb_03')._gw_node_properties['baseline_pumping']
        self.network.get_node('320101_urb_02').pumping += \
            (self.network.get_node('320101_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * hasagw_pumping
        self.network.get_node('320101_urb_03').pumping += \
            (self.network.get_node('320101_urb_03')._gw_node_properties['baseline_pumping'] / sum_pumping) * hasagw_pumping
        self.network.get_node('320301_urb_02').pumping += \
            (self.network.get_node('320301_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * hasagw_pumping
        self.network.get_node('320301_urb_03').pumping += \
            (self.network.get_node('320301_urb_03')._gw_node_properties['baseline_pumping'] / sum_pumping) * hasagw_pumping

        # awajan
        awajan_pumping = self.real_extraction['awajan'] / (30*24*60*60)
        sum_pumping = 0
        sum_pumping += self.network.get_node('130101_urb_02')._gw_node_properties['baseline_pumping'] + \
                       self.network.get_node('130201_urb_02')._gw_node_properties['baseline_pumping']
        self.network.get_node('130101_urb_02').pumping += \
            (self.network.get_node('130101_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * awajan_pumping
        self.network.get_node('130201_urb_02').pumping += \
            (self.network.get_node('130201_urb_02')._gw_node_properties['baseline_pumping'] / sum_pumping) * awajan_pumping

        # Re-allocated JV pumping:
        jv_urb_node_names = ['210501_urb_01', '240101_urb_01', '240201_urb_01', '120301_urb_01', '120101_urb_01',
                             '120201_urb_01', '310402_urb_01', '310401_urb_01', '340102_urb_01', '340101_urb_01']

        for name in jv_urb_node_names:
            jv_node = self.network.get_node(name)
            gov_name = jv_node._gw_node_properties['gov']
            diff_pumping = jv_node.pumping - jv_node._gw_node_properties['baseline_pumping']
            ratio_sum = 0
            for g in self.nodes:
                if g.component_type == 'Groundwater':
                    if g._gw_node_properties['gov'] == gov_name and g.name not in jv_urb_node_names and g.name != '340201_urb_12':
                        ratio_sum += self.wells_current_ratio[g.name]
            for g in self.nodes:
                if g.component_type == 'Groundwater':
                    if g._gw_node_properties['gov'] == gov_name and g.name not in jv_urb_node_names and g.name != '340201_urb_12':
                        g.pumping += (self.wells_current_ratio[g.name] / ratio_sum) * diff_pumping
            jv_node.pumping = jv_node._gw_node_properties['baseline_pumping']

    def setup(self, timestep):
        self.solver_status = "ok"


class GOV(JordanInstitution):
    """The Governorate class.

    Scenario-based inputs:
        population (dict)

    Intervention-based inputs:
        urban_water_prices (dict)

    Agent inputs:

    """
    description = "Water Supplier at Governorate Level"

    scenario_inputs = {
        'population': None,
    }

    intervention_inputs = {
        'urban_water_prices': None,
    }

    _properties = {
        'population_estimate_subdist': {},  # population estimates for subdistricts in governorate, indexed by subdistr.
        'demand_estimate_subdist': {},
        'demand_request': 0,  # aggregated demand requests from all subdistricts in governorate
        'supply_hours_forecast': {},
        'supply_hours': {},  # supply hours, indexed by sub-district
        'subdist_delivery': {},
    }

    def setup(self, timestep):
        pass


class AllGOV(JordanInstitution):
    description = "Institution containing all GOV institutions"


class AllNodesOfType(JordanInstitution):
    description = "Institution containing all nodes of a component type in the network"

    def __init__(self, name, component_type, network, **kwargs):
        super(AllNodesOfType, self).__init__(name, **kwargs)
        for n in network.nodes:
            if n.component_type == component_type:
                self.add_nodes(n)
        self.timestep = 0

    def setup(self, timestamp):
        self.timestep = self.timestep + 1
