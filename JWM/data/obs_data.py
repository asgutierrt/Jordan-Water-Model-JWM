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

"""This module loads observation data and adds to relevant model components.

Attributes:
    obs_data_xlsx (pandas ExcelFile object): Pandas ExcelFile object of obs_data.xlsx
    res_level_obs (pandas dataframe): Pandas dataframe of reservoir level observations
    kac_n_obs (pandas dataframe): Pandas dataframe of KAC-N system flows/transfers

"""

from JWM import get_excel_data, get_pickle, write_pickle
import math

from copy import copy

import logging
log = logging.getLogger(__name__)

# res_level_obs = obs_data_xlsx.parse("res_level")
# kac_n_obs = obs_data_xlsx.parse("kac-n source")

class Observations(object):
    """A class (non-pynsim) to store and access all observations for the Jordan model.

    The ExogenousInputs class stores inputs that define scenarios and interventions, and
    provides methods to access those inputs. Each pynsim simulation contains an ExogenousInputs
    object, which is stored as an attribute on the simulator's network. Agents should access
    exogenous inputs solely through the exogenous inputs class.

    Args:
        simulation (pynsim simulator object): object of class type JordanSimulator

    Attributes:
        scenario (string): scenario id
        intervention (string): intervention id
        scenario_inputs (dict): dictionary of scenario inputs (__init__ also sets each key as an attribute)
        intervention_inputs (dict): dictionary of intervention inputs (__init__ also sets each key as an attribute)


    """

    def __init__(self, simulation):
        self._observation_inputs = {
            'res_volume': {},
            'res_inflow': {},
            'res_evap': {},
            'res_seep': {},
            'res_overflow': {},
            'jva_transfers': {},
            'jva_deliveries': {},
            'jva_sold_new': {},
            'res_inflow_new': {},
            'jva_transfers_new': {},
            'ww_influent': {},
            'ww_effluent': {},
            'consumption_base': {},
            'amman_base': {},
            'temp': {},
            'ppt': {},
            'tanker_truck_capacity': {},
        }

        for k, v in self._observation_inputs.items():
            setattr(self, k, v)

    def calculate_inflow_averages(self):
        self.inflow_averages = {}
        for res in self._res_inflow_new.keys():
            self.inflow_averages[res] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._res_inflow_new[res].keys():
                    if year <= 2015:
                        if math.isnan(self._res_inflow_new[res][year][month+1]):
                            pass
                        else:
                            month_sum += self._res_inflow_new[res][year][month+1]
                            month_count += 1
                inflow_avg = month_sum / month_count
                self.inflow_averages[res][month+1] = inflow_avg
        for res in ['mujib_res', 'walah_res', 'tannour_res']:
            self.inflow_averages[res] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._res_inflow[res].keys():
                    if math.isnan(self._res_inflow[res][year][month+1]):
                        pass
                    else:
                        month_sum += self._res_inflow[res][year][month+1]
                        month_count += 1
                inflow_avg = month_sum / month_count
                self.inflow_averages[res][month+1] = inflow_avg

        self.inflow_averages['wehdah_res_prewar'] = {}
        self.inflow_averages['wehdah_res_postwar'] = {}
        for month in range(12):
            month_sum = 0
            month_count = 0
            for year in self._res_inflow_new['wehdah_res'].keys():
                if math.isnan(self._res_inflow_new['wehdah_res'][year][month+1]) or year >= 2012:
                    pass
                else:
                    month_sum += self._res_inflow_new['wehdah_res'][year][month+1]
                    month_count += 1
            inflow_avg = month_sum / month_count
            self.inflow_averages['wehdah_res_prewar'][month+1] = inflow_avg
        for month in range(12):
            month_sum = 0
            month_count = 0
            for year in self._res_inflow_new['wehdah_res'].keys():
                if math.isnan(self._res_inflow_new['wehdah_res'][year][month+1]) or year < 2012:
                    pass
                else:
                    month_sum += self._res_inflow_new['wehdah_res'][year][month+1]
                    month_count += 1
            inflow_avg = month_sum / month_count
            self.inflow_averages['wehdah_res_postwar'][month+1] = inflow_avg

    def calculate_cwa_transfer_averages(self):
        self.cwa_transfer_averages = {}
        self.cwa_transfer_2015 = {}
        for tr in ['wehdah_kac_n', 'yarmouk_kac_n', 'tiberias_kac_n', 'mukheiba_kac_n', 'kac_n_amman']:
            self.cwa_transfer_averages[tr] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._jva_transfers_new[tr].keys():
                    if year <= 2015:
                        if math.isnan(self._jva_transfers_new[tr][year][month+1]):
                            pass
                        else:
                            month_sum += self._jva_transfers_new[tr][year][month+1]
                            month_count += 1
                tr_avg = month_sum / month_count
                self.cwa_transfer_averages[tr][month+1] = tr_avg
        for tr in ['wehdah_kac_n', 'yarmouk_kac_n', 'tiberias_kac_n', 'mukheiba_kac_n', 'kac_n_amman']:
            self.cwa_transfer_2015[tr] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._jva_transfers_new[tr].keys():
                    if year == 2015:
                        if math.isnan(self._jva_transfers_new[tr][year][month+1]):
                            pass
                        else:
                            month_sum += self._jva_transfers_new[tr][year][month+1]
                            month_count += 1
                tr_avg = month_sum / month_count
                self.cwa_transfer_2015[tr][month+1] = tr_avg

    def calculate_ppt_averages(self):
        self.ppt_averages = {}
        for ppt in self._ppt.keys():
            self.ppt_averages[ppt] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._ppt[ppt].keys():
                    if math.isnan(self._ppt[ppt][year][month+1]):
                        pass
                    else:
                        month_sum += self._ppt[ppt][year][month+1]
                        month_count += 1
                ppt_avg = month_sum / month_count
                self.ppt_averages[ppt][month+1] = ppt_avg

    def calculate_temp_averages(self):
        self.temp_averages = {}
        for temp in self._temp.keys():
            self.temp_averages[temp] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._temp[temp].keys():
                    if math.isnan(self._temp[temp][year][month+1]):
                        pass
                    else:
                        month_sum += self._temp[temp][year][month+1]
                        month_count += 1
                temp_avg = month_sum / month_count
                self.temp_averages[temp][month+1] = temp_avg

    def calculate_volume_averages(self):
        self.volume_averages = {}
        for res in self._res_volume.keys():
            self.volume_averages[res] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._res_volume[res].keys():
                    if math.isnan(self._res_volume[res][year][month+1]):
                        pass
                    else:
                        month_sum += self._res_volume[res][year][month+1]
                        month_count += 1
                volume_avg = month_sum / month_count
                self.volume_averages[res][month+1] = volume_avg

    def calculate_evap_averages(self):
        self.evap_averages = {}
        for res in self._res_evap.keys():
            self.evap_averages[res] = {}
            for month in range(12):
                month_sum = 0
                month_count = 0
                for year in self._res_evap[res].keys():
                    if math.isnan(self._res_evap[res][year][month+1]):
                        pass
                    else:
                        month_sum += self._res_evap[res][year][month+1]
                        month_count += 1
                evap_avg = month_sum / month_count
                self.evap_averages[res][month+1] = evap_avg
        # for wehdah and tannour, no evap data available from Hagan files (calculate using evap calculation and average volumes)
        for res in ['wehdah_res', 'tannour_res', 'mujib_res', 'walah_res', 'karameh_res']:
            self.evap_averages[res] = {}
            for month in range(12):
                res_vol = self.volume_averages[res][month+1]
                if res == 'wehdah_res':
                    temp = self.temp_averages['AL0035'][month+1]
                    pan_evap = (temp * .4286) + .5015
                    surf_area = (7e-16 * (res_vol**3)) + (-2e-8 * (res_vol**2)) + (.159 * res_vol)
                else:
                    temp = self.temp_averages['AM0007'][month+1]
                    pan_evap = (temp * .6272) - 4.5345
                    surf_area = (9e-17 * (res_vol ** 3)) + (-3e-9 * (res_vol ** 2)) + (.0896 * res_vol)
                evap_calc = surf_area * pan_evap / (1.3 * 1000)
                self.evap_averages[res][month+1] = evap_calc

    def calculate_ww_influent_effluent_ratios(self):
        self.wwtp_ratios = {}
        sum = 0
        count = 0
        for wwtp in self._ww_influent.keys():
            for year in self._ww_influent[wwtp].keys():
                for month in range(12):
                    if self._ww_influent[wwtp][year][month+1] != 0 and self._ww_effluent[wwtp][year][month+1] != 0 and self._ww_effluent[wwtp][year][month+1]/self._ww_influent[wwtp][year][month+1] <= 1:
                        ratio = self._ww_effluent[wwtp][year][month+1]/self._ww_influent[wwtp][year][month+1]
                        sum += ratio
                        count += 1
            self.wwtp_ratios[wwtp] = sum / count
