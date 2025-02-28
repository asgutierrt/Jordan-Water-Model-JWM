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

from pynsim import Engine
from JWM import get_excel_data
import numpy as np

swat_inputs_xlsx = get_excel_data("swat_inputs.xlsx")

class SWInflowEngine(Engine):
    name = """Calculates inflows to reservoirs from surface water reservoir"""

    _swat_inputs = {
        'swat_inputs': {},
    }

    def run(self):
        """
            The target of this are all reservoir nodes in the system
        """

        # Load SWAT data if first timestep
        if self.target.network.current_timestep_idx == 0:
            self._swat_inputs = {'swat_inputs': {}, } 

            for k, v in self._swat_inputs.items():
                inputs_all = swat_inputs_xlsx.parse(k)
                syria_lu_string = self.target.network.parameters.sw['rcp'] + ' ' + self.target.network.parameters.sw['syria_lu']
                inputs = inputs_all[(inputs_all.scenario == self.target.network.parameters.sw['rcp']) | (inputs_all.scenario == syria_lu_string)]
                name = "_" + k
                value = {}
                if inputs['index3_value'].count() != 0:
                    value = dict((i, float('nan')) for i in inputs['index1_value'].values)
                    for i1 in value.keys():
                        value[i1] = dict((i, 'na') for i in inputs['index2_value'].values)
                        for i2 in inputs['index2_value'].unique():
                            value[i1][i2] = dict((i, float('nan')) for i in inputs['index3_value'].values)
                            for i3 in inputs['index3_value'].unique():
                                try:
                                    value[i1][i2][i3] = inputs[
                                        (inputs.index1_value == i1) & (inputs.index2_value == i2) & (
                                        inputs.index3_value == i3)]['value'].values[0]
                                except IndexError:
                                    pass

                elif inputs['index2_value'].count() != 0:
                    value = dict((i, float('nan')) for i in inputs['index1_value'].values)
                    for i1 in value.keys():
                        value[i1] = dict((i, float('nan')) for i in inputs['index2_value'].values)
                        for i2 in inputs['index2_value'].unique():
                            try:
                                value[i1][i2] = \
                                inputs[(inputs.index1_value == i1) & (inputs.index2_value == i2)]['value'].values[0]
                            except IndexError:
                                pass

                elif inputs['index1_value'].count() != 0:
                    value = dict((i, float('nan')) for i in inputs['index1_value'].values)
                    for i1 in value.keys():
                        try:
                            value[i1] = inputs[(inputs.index1_value == i1)]['value'].values[0]
                        except IndexError:
                            pass

                setattr(self, name, value)


        month = self.target.network.current_timestep.month
        year = self.target.network.current_timestep.year
        for res in self.target.nodes:
            # if reservoir inflow is modeled in SWAT, retrieve the monthly inflow data
            if res.name in self._swat_inputs.keys():
                res.inflow = self._swat_inputs[res.name][year][month] * self.target.network.parameters.sw['inflow_factor']
            else:
                # create dictionary to assign non-modeled reservoirs with modeled ones
                non_swat_res = {
                    'arab_res': 'shueib_res',
                    'ziglab_res': 'shueib_res',
                    'shurabil_res': 'kafrein_res',
                    'tannour_res': 'mujib_res',
                    'walah_res': 'mujib_res',
                }
                # if reservoir inflow is not modeled in SWAT, adjust inflows based on assigned SWAT inflow
                if res.name in non_swat_res.keys():
                    if self.target.network.observations.inflow_averages[non_swat_res[res.name]][month] > 0.0:
                        inflow_ratio = self._swat_inputs[non_swat_res[res.name]][year][month] / self.target.network.observations.inflow_averages[non_swat_res[res.name]][month]
                    else:
                        inflow_ratio = self._swat_inputs[non_swat_res[res.name]][year][month+1] / self.target.network.observations.inflow_averages[non_swat_res[res.name]][month+1]
                    res.inflow = self.target.network.observations.inflow_averages[res.name][month] * inflow_ratio * self.target.network.parameters.sw['inflow_factor']
                else:
                    res.inflow = 0


class ReservoirBalanceEngine(Engine):
    name = """Runs water balance on reservoirs"""

    def run(self):
        """
            The target of this are all reservoir nodes in the system
        """

        for res in self.target.nodes:
            if res.name == 'walah_res' or res.name == 'mujib_res':
                res.run_mujib_walah_balance()

class WWEngine(Engine):
    name = """Runs wastewater flow engine"""

    def run(self):
        """
            The target of this are all wastewater nodes in the system
        """

        # For first time period, calculate gov wastewater ratios
        if self.target.network.current_timestep_idx == 0:
            self.gov_ww_totals = {}
            for wwtp in self.target.nodes:
                gov_count = 0
                for wwtp_gov in wwtp.served_govs:
                    if gov_count == 0:
                        if wwtp_gov in self.gov_ww_totals.keys():
                            self.gov_ww_totals[wwtp_gov] += wwtp.gov1_mcm_2012
                        else:
                            self.gov_ww_totals[wwtp_gov] = wwtp.gov1_mcm_2012
                    else:
                        if wwtp_gov in self.gov_ww_totals.keys():
                            self.gov_ww_totals[wwtp_gov] += wwtp.gov2_mcm_2012
                        else:
                            self.gov_ww_totals[wwtp_gov] = wwtp.gov2_mcm_2012
                    gov_count += 1

        month = self.target.network.current_timestep.month
        year = self.target.network.current_timestep.year

        consumption_effluent_ratio = .4714

        ww_ratios_data = self.target.network.get_institution("waj").WAJ_inputs.parse('consumption_to_wastewater')[0:12]
        CI_ratios = ww_ratios_data.set_index("year")["consumption-influent-ratio"].to_dict()
        IE_ratio = ww_ratios_data["influent-effluent-ratio"][0]
        CI_year = min(year, 2016)
        consumption_influent_ratio = CI_ratios[CI_year]

        influent_effluent_ratio = IE_ratio

        monthly_adj = {
            1:1,
            2:1,
            3:.95,
            4:0.9,
            5:0.9,
            6:0.9,
            7:.85,
            8:.85,
            9:.9,
            10:.9,
            11:1,
            12:1,
        }

        # Calculate current consumption for users connected to sewers
        haw = self.target.network.get_institution('human_agent_wrapper')
        self.sewage_consumption = {}
        self.sewage_percent = {}
        if self.target.network.current_timestep_idx == 0:
            self.sewage_percent_initial = {}

        for gov in haw.govs:
            consumption = 0
            total_consumption = 0
            for hh in haw.hh_agents:
                if gov.gov_name == hh.gov_name:
                    if hh.is_sewage:
                        consumption += hh.piped_consumption * hh.represented_units * (365 / 12)
                        consumption += hh.tanker_consumption * hh.represented_units * (365 / 12)
                        total_consumption += hh.piped_consumption * hh.represented_units * (365 / 12)
                        total_consumption += hh.tanker_consumption * hh.represented_units * (365 / 12)
                    else:
                        total_consumption += hh.piped_consumption * hh.represented_units * (365 / 12)
                        total_consumption += hh.tanker_consumption * hh.represented_units * (365 / 12)

            for rf in haw.rf_agents:
                if gov.gov_name == rf.gov_name:
                    if rf.is_sewage:
                        consumption += rf.piped_consumption * rf.represented_units * (365 / 12)
                        consumption += rf.tanker_consumption * rf.represented_units * (365 / 12)
                        total_consumption += rf.piped_consumption * rf.represented_units * (365 / 12)
                        total_consumption += rf.tanker_consumption * rf.represented_units * (365 / 12)
                    else:
                        total_consumption += rf.piped_consumption * rf.represented_units * (365 / 12)
                        total_consumption += rf.tanker_consumption * rf.represented_units * (365 / 12)

            co_consumption = 0
            total_co_consumption = 0
            for co in haw.co_agents:
                if gov.gov_id == co.gov:
                    co_consumption += co.piped_consumption * co.represented_units * (365 / 12) * co.sewage_rate
                    co_consumption += co.tanker_consumption * co.represented_units * (365 / 12) * co.sewage_rate
                    total_co_consumption += co.piped_consumption * \
                                            co.represented_units * (365 / 12)
                    total_co_consumption += co.tanker_consumption * \
                                            co.represented_units * (365 / 12)

            sewage_ratio = consumption / total_consumption
            co_sewage_ratio = co_consumption / total_co_consumption

            consumption += co_consumption
            consumption += gov.total_in_piped_consumption * co_sewage_ratio
            consumption += gov.total_in_surface_consumption * co_sewage_ratio
            consumption += gov.total_in_well_consumption * co_sewage_ratio

            self.sewage_consumption[gov.gov_name] = consumption * self.target.network.parameters.ww['ww_factor']
            self.sewage_percent[gov.gov_name] = consumption / (total_consumption + total_co_consumption)

            if self.target.network.current_timestep_idx == 0:
                self.sewage_percent_initial[gov.gov_name] = consumption / (total_consumption + total_co_consumption)

            if self.sewage_percent[gov.gov_name] < .90:
                add_percent = ((self.target.network.current_timestep.year - 2016) * .02) #Assume 2 percent growth rate in connections per year
                new_percent = self.sewage_percent_initial[gov.gov_name] + add_percent
                if new_percent <= .90:
                    self.sewage_consumption[gov.gov_name] = new_percent * (total_consumption + total_co_consumption)* self.target.network.parameters.ww['ww_factor']
                else:
                    self.sewage_consumption[gov.gov_name] = .90 * (total_consumption + total_co_consumption)* self.target.network.parameters.ww['ww_factor']


        # Calculation for plants with existing effluent flows
        for wwtp in self.target.nodes:
            wwtp.effluent_history.append({})
            wwtp.influent = 0
            gov_count = 0
            for gov in wwtp.served_govs:
                dest_count = 0
                for dest in wwtp.effluent_dest:
                    if gov_count == 0 and dest_count == 0:
                        wwtp.effluent[dest] = (self.sewage_consumption[gov[0:3]] * wwtp.gov1_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest1_percent/100) * consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month]
                        wwtp.influent += (self.sewage_consumption[gov[0:3]] * wwtp.gov1_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest1_percent/100) * consumption_influent_ratio * monthly_adj[month]
                    if gov_count == 1 and dest_count == 0:
                        wwtp.effluent[dest] += (self.sewage_consumption[gov[0:3]] * wwtp.gov2_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest1_percent/100) * consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month]
                        wwtp.influent += (self.sewage_consumption[gov[0:3]] * wwtp.gov2_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest1_percent/100) * consumption_influent_ratio * monthly_adj[month]
                    if gov_count == 0 and dest_count == 1:
                        wwtp.effluent[dest] = (self.sewage_consumption[gov[0:3]] * wwtp.gov1_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest2_percent/100) * consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month]
                        wwtp.influent += (self.sewage_consumption[gov[0:3]] * wwtp.gov1_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest2_percent/100) * consumption_influent_ratio * monthly_adj[month]
                    if gov_count == 1 and dest_count == 1:
                        wwtp.effluent[dest] += (self.sewage_consumption[gov[0:3]] * wwtp.gov2_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest2_percent/100) * consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month]
                        wwtp.influent += (self.sewage_consumption[gov[0:3]] * wwtp.gov2_mcm_2012 / \
                            self.gov_ww_totals[gov]) * (wwtp.dest2_percent/100) * consumption_influent_ratio * monthly_adj[month]
                    dest_count += 1
                gov_count += 1
            for dest in wwtp.effluent_dest:
                wwtp.effluent_history[-1][dest] = wwtp.effluent[dest]

        # Adjust reservoir inflows based on effluent calculations
        for res in self.target.network.get_institution('all_reservoir_nodes').nodes:
            if res.name == 'talal_res':
                inflow_adj = 0
                inflow_adj += (self.sewage_consumption['jar'] - self.target.network.observations._consumption_base['jarash'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] # Jarash-east and Al-merad WWTPs
                inflow_adj += (self.sewage_consumption['amm'] - self.target.network.observations._consumption_base['amman'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (0.9*.94/self.gov_ww_totals['amman']) # Abu Nuseir WWTP
                inflow_adj += (self.sewage_consumption['bal'] - self.target.network.observations._consumption_base['balqa'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (2.6*.82/self.gov_ww_totals['balqa']) # Ba'qah WWTP
                inflow_adj += (self.sewage_consumption['amm'] - self.target.network.observations._consumption_base['amman'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (65*.84/self.gov_ww_totals['amman']) # Samra WWTP
                res.inflow += inflow_adj

            if res.name == 'kafrein_res':
                inflow_adj = 0
                inflow_adj += (self.sewage_consumption['amm'] - self.target.network.observations._consumption_base['amman'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (1.5*.94/self.gov_ww_totals['amman']) # # Wadi Al Sir WWTP

            if res.name == 'shueib_res':
                inflow_adj = 0
                inflow_adj += (self.sewage_consumption['bal'] - self.target.network.observations._consumption_base['balqa'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (.79*.95/self.gov_ww_totals['balqa']) # # Fuheis WWTP
                inflow_adj += (self.sewage_consumption['bal'] - self.target.network.observations._consumption_base['balqa'][2014][month]) * \
                    consumption_influent_ratio * influent_effluent_ratio * monthly_adj[month] * (2.4*.94/self.gov_ww_totals['balqa']) # # Salt WWTP

