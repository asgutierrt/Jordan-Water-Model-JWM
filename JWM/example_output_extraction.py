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

# This is an example script file for extracting model results from a JWM run to csv files. The script stores relevant agent/network
# property histories for several attributes of interest across the system. The script can be applied to a pynsim
# simulation object ("s" in this example) that has been executed in an interactive console and completed a model run,
# or directly integrated into the main.py file.

import sys
import xlsxwriter
import xlrd
import csv
import numpy as np
from datetime import datetime

now = datetime.now()
nowstring = now.strftime("%Y-%m-%d-%H%M%S")

filename = 'outputs\\household' + nowstring + '.xlsx'

newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Household Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0

for t in s.network.timesteps:
    for hh in haw.hh_agents:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'subdist')
            newsheet.write(row, 2, 'gov')
            newsheet.write(row, 3, 'timestep')
            newsheet.write(row, 4, 'date')
            newsheet.write(row, 5, 'year')
            newsheet.write(row, 6, 'month')
            newsheet.write(row, 7, 'represented_units')
            newsheet.write(row, 8, 'income')
            newsheet.write(row, 9, 'hnum')
            newsheet.write(row, 10, 'rururb')
            newsheet.write(row, 11, 'piped')
            newsheet.write(row, 12, 'tanker')
            newsheet.write(row, 13, 'cs_diff')
            newsheet.write(row, 14, 'final_tanker_price')
            newsheet.write(row, 15, 'distance_average')
            newsheet.write(row, 16, 'cs_diff_per_cap')
            newsheet.write(row, 17, 'sewage')
            newsheet.write(row, 18, 'expenditure')
            newsheet.write(row, 19, 'lifeline')
            newsheet.write(row, 20, 'refugee')
            newsheet.write(row, 21, 'csf_run_no')
            newsheet.write(row, 22, 'cs_diff_per_unit')
            newsheet.write(row, 23, 'piped_expenditure')
            newsheet.write(row, 24, 'tanker_expenditure')
            newsheet.write(row, 25, 'absolute_consumer_surplus')
            newsheet.write(row, 26, 'conseq_months_below_30_lcd')
            newsheet.write(row, 27, 'conseq_months_below_40_lcd')
            newsheet.write(row, 28, 'conseq_months_below_50_lcd')
            newsheet.write(row, 29, 'conseq_months_piped_below_30_lcd')
            newsheet.write(row, 30, 'conseq_months_piped_below_40_lcd')
            newsheet.write(row, 31, 'conseq_months_piped_below_50_lcd')

        try:
            cs_diff_per_cap = hh.get_history('consumer_surplus_diff')[t_idx] / (hh.get_history('hnum')[t_idx])
        except TypeError:
            cs_diff_per_cap = ''

        cs_diff_per_cap = hh.get_history('consumer_surplus_diff')[t_idx] / (hh.get_history('hnum')[t_idx])
        if np.isnan(cs_diff_per_cap):
            cs_diff_per_cap = 'nan'
        if np.isinf(cs_diff_per_cap):
            cs_diff_per_cap = 'inf'

        piped_consumption = hh.get_history('piped_consumption')[t_idx] * hh.get_history('represented_units')[
            t_idx] * (365 / 12)
        tanker_consumption = hh.get_history('tanker_consumption')[t_idx] * hh.get_history('represented_units')[
            t_idx] * (365 / 12)

        if hh.is_sewage:
            sewage = 'yes'
        else:
            sewage = 'no'

        newsheet.write(row + 1, 0, hh.name)
        newsheet.write(row + 1, 1, hh.name[9:15])
        newsheet.write(row + 1, 2, hh.gov_name)
        newsheet.write(row + 1, 3, t_idx + 1)
        newsheet.write(row + 1, 4, t, date_format)
        newsheet.write(row + 1, 5, t.year)
        newsheet.write(row + 1, 6, t.month)
        newsheet.write(row + 1, 7, hh.get_history('represented_units')[t_idx])
        newsheet.write(row + 1, 8, hh.params['income'])
        newsheet.write(row + 1, 9, hh.get_history('hnum')[t_idx])
        newsheet.write(row + 1, 10, hh.params['rururb'])
        newsheet.write(row + 1, 11, piped_consumption)
        newsheet.write(row + 1, 12, tanker_consumption)
        newsheet.write(row + 1, 13, hh.get_history('consumer_surplus_diff')[t_idx])
        newsheet.write(row + 1, 14, hh.get_history('final_tanker_price')[t_idx])
        newsheet.write(row + 1, 15, hh.get_history('distance_average')[t_idx])
        newsheet.write(row + 1, 16, cs_diff_per_cap)
        newsheet.write(row + 1, 17, sewage)
        newsheet.write(row + 1, 18, hh.get_history('expenditure')[t_idx])
        newsheet.write(row + 1, 19, hh.get_history('lifeline_consumption')[t_idx])
        newsheet.write(row + 1, 20, 'no')
        newsheet.write(row + 1, 21, sys.argv[1])
        newsheet.write(row + 1, 22, hh.get_history('consumer_surplus_diff_per_unit')[t_idx])
        newsheet.write(row + 1, 23, hh.get_history('piped_expenditure')[t_idx])
        newsheet.write(row + 1, 24, hh.get_history('tanker_expenditure')[t_idx])
        newsheet.write(row + 1, 25, hh.get_history('absolute_consumer_surplus')[t_idx])
        newsheet.write(row + 1, 26, hh.get_history('conseq_months_below_30_lcd')[t_idx])
        newsheet.write(row + 1, 27, hh.get_history('conseq_months_below_40_lcd')[t_idx])
        newsheet.write(row + 1, 28, hh.get_history('conseq_months_below_50_lcd')[t_idx])
        newsheet.write(row + 1, 29, hh.get_history('conseq_months_piped_below_30_lcd')[t_idx])
        newsheet.write(row + 1, 30, hh.get_history('conseq_months_piped_below_40_lcd')[t_idx])
        newsheet.write(row + 1, 31, hh.get_history('conseq_months_piped_below_50_lcd')[t_idx])

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Household Agents')
csv_file = open('outputs\\household'+nowstring+'.csv','wb')
wr = csv.writer(csv_file,quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\refugee' + nowstring + '.xlsx'

newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Refugee Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for hh in haw.rf_agents:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'subdist')
            newsheet.write(row, 2, 'gov')
            newsheet.write(row, 3, 'timestep')
            newsheet.write(row, 4, 'date')
            newsheet.write(row, 5, 'year')
            newsheet.write(row, 6, 'month')
            newsheet.write(row, 7, 'represented_units')
            newsheet.write(row, 8, 'income')
            newsheet.write(row, 9, 'hnum')
            newsheet.write(row, 10, 'rururb')
            newsheet.write(row, 11, 'piped')
            newsheet.write(row, 12, 'tanker')
            newsheet.write(row, 13, 'cs_diff')
            newsheet.write(row, 14, 'final_tanker_price')
            newsheet.write(row, 15, 'distance_average')
            newsheet.write(row, 16, 'cs_diff_per_cap')
            newsheet.write(row, 17, 'sewage')
            newsheet.write(row, 18, 'expenditure')
            newsheet.write(row, 19, 'lifeline')
            newsheet.write(row, 20, 'refugee')
            newsheet.write(row, 21, 'csf_run_no')
            newsheet.write(row, 22, 'cs_diff_per_unit')
            newsheet.write(row, 23, 'piped_expenditure')
            newsheet.write(row, 24, 'tanker_expenditure')
            newsheet.write(row, 25, 'absolute_consumer_surplus')
            newsheet.write(row, 26, 'conseq_months_below_30_lcd')
            newsheet.write(row, 27, 'conseq_months_below_40_lcd')
            newsheet.write(row, 28, 'conseq_months_below_50_lcd')
            newsheet.write(row, 29, 'conseq_months_piped_below_30_lcd')
            newsheet.write(row, 30, 'conseq_months_piped_below_40_lcd')
            newsheet.write(row, 31, 'conseq_months_piped_below_50_lcd')

        try:
            cs_diff_per_cap = hh.get_history('consumer_surplus_diff')[t_idx] / (hh.get_history('hnum')[t_idx])
        except TypeError:
            cs_diff_per_cap = ''

        cs_diff_per_cap = hh.get_history('consumer_surplus_diff')[t_idx] / (hh.get_history('hnum')[t_idx])
        if np.isnan(cs_diff_per_cap):
            cs_diff_per_cap = 'nan'
        if np.isinf(cs_diff_per_cap):
            cs_diff_per_cap = 'inf'

        piped_consumption = hh.get_history('piped_consumption')[t_idx] * hh.get_history('represented_units')[
            t_idx] * (365 / 12)
        tanker_consumption = hh.get_history('tanker_consumption')[t_idx] * hh.get_history('represented_units')[
            t_idx] * (365 / 12)

        if hh.is_sewage:
            sewage = 'yes'
        else:
            sewage = 'no'

        newsheet.write(row + 1, 0, hh.name)
        newsheet.write(row + 1, 1, hh.name[9:15])
        newsheet.write(row + 1, 2, hh.gov_name)
        newsheet.write(row + 1, 3, t_idx + 1)
        newsheet.write(row + 1, 4, t, date_format)
        newsheet.write(row + 1, 5, t.year)
        newsheet.write(row + 1, 6, t.month)
        newsheet.write(row + 1, 7, hh.get_history('represented_units')[t_idx])
        newsheet.write(row + 1, 8, hh.params['income'])
        newsheet.write(row + 1, 9, hh.get_history('hnum')[t_idx])
        newsheet.write(row + 1, 10, hh.params['rururb'])
        newsheet.write(row + 1, 11, piped_consumption)
        newsheet.write(row + 1, 12, tanker_consumption)
        newsheet.write(row + 1, 13, hh.get_history('consumer_surplus_diff')[t_idx])
        newsheet.write(row + 1, 14, hh.get_history('final_tanker_price')[t_idx])
        newsheet.write(row + 1, 15, hh.get_history('distance_average')[t_idx])
        newsheet.write(row + 1, 16, cs_diff_per_cap)
        newsheet.write(row + 1, 17, sewage)
        newsheet.write(row + 1, 18, hh.get_history('expenditure')[t_idx])
        newsheet.write(row + 1, 19, hh.get_history('lifeline_consumption')[t_idx])
        newsheet.write(row + 1, 20, 'yes')
        newsheet.write(row + 1, 21, sys.argv[1])
        try:
            newsheet.write(row + 1, 22, hh.get_history('consumer_surplus_diff_per_unit')[t_idx])
        except TypeError:
            newsheet.write(row + 1, 22, '')
        newsheet.write(row + 1, 23, hh.get_history('piped_expenditure')[t_idx])
        newsheet.write(row + 1, 24, hh.get_history('tanker_expenditure')[t_idx])
        newsheet.write(row + 1, 25, hh.get_history('absolute_consumer_surplus')[t_idx])
        newsheet.write(row + 1, 26, hh.get_history('conseq_months_below_30_lcd')[t_idx])
        newsheet.write(row + 1, 27, hh.get_history('conseq_months_below_40_lcd')[t_idx])
        newsheet.write(row + 1, 28, hh.get_history('conseq_months_below_50_lcd')[t_idx])
        newsheet.write(row + 1, 29, hh.get_history('conseq_months_piped_below_30_lcd')[t_idx])
        newsheet.write(row + 1, 30, hh.get_history('conseq_months_piped_below_40_lcd')[t_idx])
        newsheet.write(row + 1, 31, hh.get_history('conseq_months_piped_below_50_lcd')[t_idx])

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Refugee Agents')
csv_file = open('outputs\\refugee'+nowstring+'.csv','wb')
wr = csv.writer(csv_file,quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\commercialpt1_' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Commercial Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0
for t in s.network.timesteps:
    if t_idx <= 300:
        for co in haw.co_agents:
            if row == 0:
                newsheet.write(row, 0, 'name')
                newsheet.write(row, 1, 'subdist')
                newsheet.write(row, 2, 'gov')
                newsheet.write(row, 3, 'timestep')
                newsheet.write(row, 4, 'date')
                newsheet.write(row, 5, 'year')
                newsheet.write(row, 6, 'month')
                newsheet.write(row, 7, 'represented_units')
                newsheet.write(row, 8, 'piped')
                newsheet.write(row, 9, 'tanker')
                newsheet.write(row, 10, 'cs_diff')
                newsheet.write(row, 11, 'final_tanker_price')
                newsheet.write(row, 12, 'distance_average')
                newsheet.write(row, 13, 'cs_diff_per_est')
                newsheet.write(row, 14, 'sewage_rate')
                newsheet.write(row, 15, 'storage_cap')
                newsheet.write(row, 16, 'lifeline')
                newsheet.write(row, 17, 'csf_run_no')
                newsheet.write(row, 18, 'cs_diff_per_unit')
                newsheet.write(row, 19, 'piped_expenditure')
                newsheet.write(row, 20, 'tanker_expenditure')
                newsheet.write(row, 21, 'expenditure')
                newsheet.write(row, 22, 'absolute_consumer_surplus')

            cs_diff_per_est = co.get_history('consumer_surplus_diff')[t_idx]
            if np.isnan(cs_diff_per_est):
                cs_diff_per_est = 'nan'

            subdist_name = 'subdist_' + co.name[9:15] + '_wrapper'
            gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate

            piped_consumption = co.get_history('piped_consumption')[t_idx] * co.get_history('represented_units')[t_idx] *(365/12)
            tanker_consumption = co.get_history('tanker_consumption')[t_idx] * co.get_history('represented_units')[t_idx] *(365/12)

            newsheet.write(row+1, 0, co.name)
            newsheet.write(row+1, 1, co.name[9:15])
            newsheet.write(row+1, 2, gov_agent.gov_name)
            newsheet.write(row+1, 3, t_idx+1)
            newsheet.write(row+1, 4, t, date_format)
            newsheet.write(row+1, 5, t.year)
            newsheet.write(row+1, 6, t.month)
            newsheet.write(row+1, 7, co.get_history('represented_units')[t_idx])
            newsheet.write(row+1, 8, piped_consumption)
            newsheet.write(row+1, 9, tanker_consumption)
            newsheet.write(row+1, 10, co.get_history('consumer_surplus_diff')[t_idx])
            newsheet.write(row+1, 11, co.get_history('final_tanker_price')[t_idx])
            newsheet.write(row+1, 12, co.get_history('distance_average')[t_idx])
            newsheet.write(row+1, 13, cs_diff_per_est)
            newsheet.write(row+1, 14, co.sewage_rate)
            newsheet.write(row + 1, 15, co.params[7])
            newsheet.write(row + 1, 16, co.get_history('lifeline_consumption')[t_idx])
            newsheet.write(row+1, 17, sys.argv[1])
            newsheet.write(row + 1, 18, co.get_history('consumer_surplus_diff_per_unit')[t_idx])
            newsheet.write(row + 1, 19, co.get_history('piped_expenditure')[t_idx])
            newsheet.write(row + 1, 20, co.get_history('tanker_expenditure')[t_idx])
            newsheet.write(row + 1, 21, co.get_history('expenditure')[t_idx])
            newsheet.write(row + 1, 22, co.get_history('absolute_consumer_surplus')[t_idx])

            row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Commercial Agents')
csv_file = open('outputs\\commercialpt1_'+nowstring+'.csv','wb')
wr = csv.writer(csv_file,quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\commercialpt2_' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Commercial Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0
for t in s.network.timesteps:
    if t_idx > 300 and t_idx <= 600:
        for co in haw.co_agents:
            if row == 0:
                newsheet.write(row, 0, 'name')
                newsheet.write(row, 1, 'subdist')
                newsheet.write(row, 2, 'gov')
                newsheet.write(row, 3, 'timestep')
                newsheet.write(row, 4, 'date')
                newsheet.write(row, 5, 'year')
                newsheet.write(row, 6, 'month')
                newsheet.write(row, 7, 'represented_units')
                newsheet.write(row, 8, 'piped')
                newsheet.write(row, 9, 'tanker')
                newsheet.write(row, 10, 'cs_diff')
                newsheet.write(row, 11, 'final_tanker_price')
                newsheet.write(row, 12, 'distance_average')
                newsheet.write(row, 13, 'cs_diff_per_est')
                newsheet.write(row, 14, 'sewage_rate')
                newsheet.write(row, 15, 'storage_cap')
                newsheet.write(row, 16, 'lifeline')
                newsheet.write(row, 17, 'csf_run_no')
                newsheet.write(row, 18, 'cs_diff_per_unit')
                newsheet.write(row, 19, 'piped_expenditure')
                newsheet.write(row, 20, 'tanker_expenditure')
                newsheet.write(row, 21, 'expenditure')
                newsheet.write(row, 22, 'absolute_consumer_surplus')

            cs_diff_per_est = co.get_history('consumer_surplus_diff')[t_idx]
            if np.isnan(cs_diff_per_est):
                cs_diff_per_est = 'nan'

            subdist_name = 'subdist_' + co.name[9:15] + '_wrapper'
            gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate

            piped_consumption = co.get_history('piped_consumption')[t_idx] * co.get_history('represented_units')[
                t_idx] * (365 / 12)
            tanker_consumption = co.get_history('tanker_consumption')[t_idx] * co.get_history('represented_units')[
                t_idx] * (365 / 12)

            newsheet.write(row + 1, 0, co.name)
            newsheet.write(row + 1, 1, co.name[9:15])
            newsheet.write(row + 1, 2, gov_agent.gov_name)
            newsheet.write(row + 1, 3, t_idx + 1)
            newsheet.write(row + 1, 4, t, date_format)
            newsheet.write(row + 1, 5, t.year)
            newsheet.write(row + 1, 6, t.month)
            newsheet.write(row + 1, 7, co.get_history('represented_units')[t_idx])
            newsheet.write(row + 1, 8, piped_consumption)
            newsheet.write(row + 1, 9, tanker_consumption)
            newsheet.write(row + 1, 10, co.get_history('consumer_surplus_diff')[t_idx])
            newsheet.write(row + 1, 11, co.get_history('final_tanker_price')[t_idx])
            newsheet.write(row + 1, 12, co.get_history('distance_average')[t_idx])
            newsheet.write(row + 1, 13, cs_diff_per_est)
            newsheet.write(row + 1, 14, co.sewage_rate)
            newsheet.write(row + 1, 15, co.params[7])
            newsheet.write(row + 1, 16, co.get_history('lifeline_consumption')[t_idx])
            newsheet.write(row + 1, 17, sys.argv[1])
            newsheet.write(row + 1, 18, co.get_history('consumer_surplus_diff_per_unit')[t_idx])
            newsheet.write(row + 1, 19, co.get_history('piped_expenditure')[t_idx])
            newsheet.write(row + 1, 20, co.get_history('tanker_expenditure')[t_idx])
            newsheet.write(row + 1, 21, co.get_history('expenditure')[t_idx])
            newsheet.write(row + 1, 22, co.get_history('absolute_consumer_surplus')[t_idx])

            row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Commercial Agents')
csv_file = open('outputs\\commercialpt2_' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\commercialpt3_' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Commercial Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0
for t in s.network.timesteps:
    if t_idx > 600:
        for co in haw.co_agents:
            if row == 0:
                newsheet.write(row, 0, 'name')
                newsheet.write(row, 1, 'subdist')
                newsheet.write(row, 2, 'gov')
                newsheet.write(row, 3, 'timestep')
                newsheet.write(row, 4, 'date')
                newsheet.write(row, 5, 'year')
                newsheet.write(row, 6, 'month')
                newsheet.write(row, 7, 'represented_units')
                newsheet.write(row, 8, 'piped')
                newsheet.write(row, 9, 'tanker')
                newsheet.write(row, 10, 'cs_diff')
                newsheet.write(row, 11, 'final_tanker_price')
                newsheet.write(row, 12, 'distance_average')
                newsheet.write(row, 13, 'cs_diff_per_est')
                newsheet.write(row, 14, 'sewage_rate')
                newsheet.write(row, 15, 'storage_cap')
                newsheet.write(row, 16, 'lifeline')
                newsheet.write(row, 17, 'csf_run_no')
                newsheet.write(row, 18, 'cs_diff_per_unit')
                newsheet.write(row, 19, 'piped_expenditure')
                newsheet.write(row, 20, 'tanker_expenditure')
                newsheet.write(row, 21, 'expenditure')
                newsheet.write(row, 22, 'absolute_consumer_surplus')

            cs_diff_per_est = co.get_history('consumer_surplus_diff')[t_idx]
            if np.isnan(cs_diff_per_est):
                cs_diff_per_est = 'nan'

            subdist_name = 'subdist_' + co.name[9:15] + '_wrapper'
            gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate

            piped_consumption = co.get_history('piped_consumption')[t_idx] * co.get_history('represented_units')[
                t_idx] * (365 / 12)
            tanker_consumption = co.get_history('tanker_consumption')[t_idx] * co.get_history('represented_units')[
                t_idx] * (365 / 12)

            newsheet.write(row + 1, 0, co.name)
            newsheet.write(row + 1, 1, co.name[9:15])
            newsheet.write(row + 1, 2, gov_agent.gov_name)
            newsheet.write(row + 1, 3, t_idx + 1)
            newsheet.write(row + 1, 4, t, date_format)
            newsheet.write(row + 1, 5, t.year)
            newsheet.write(row + 1, 6, t.month)
            newsheet.write(row + 1, 7, co.get_history('represented_units')[t_idx])
            newsheet.write(row + 1, 8, piped_consumption)
            newsheet.write(row + 1, 9, tanker_consumption)
            newsheet.write(row + 1, 10, co.get_history('consumer_surplus_diff')[t_idx])
            newsheet.write(row + 1, 11, co.get_history('final_tanker_price')[t_idx])
            newsheet.write(row + 1, 12, co.get_history('distance_average')[t_idx])
            newsheet.write(row + 1, 13, cs_diff_per_est)
            newsheet.write(row + 1, 14, co.sewage_rate)
            newsheet.write(row + 1, 15, co.params[7])
            newsheet.write(row + 1, 16, co.get_history('lifeline_consumption')[t_idx])
            newsheet.write(row + 1, 17, sys.argv[1])
            newsheet.write(row + 1, 18, co.get_history('consumer_surplus_diff_per_unit')[t_idx])
            newsheet.write(row + 1, 19, co.get_history('piped_expenditure')[t_idx])
            newsheet.write(row + 1, 20, co.get_history('tanker_expenditure')[t_idx])
            newsheet.write(row + 1, 21, co.get_history('expenditure')[t_idx])
            newsheet.write(row + 1, 22, co.get_history('absolute_consumer_surplus')[t_idx])

            row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Commercial Agents')
csv_file = open('outputs\\commercialpt3_' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\industrial' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Industrial Agents')
haw = s.network.get_institution('human_agent_wrapper')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for ind in haw.in_agents:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'subdist')
            newsheet.write(row, 2, 'gov')
            newsheet.write(row, 3, 'timestep')
            newsheet.write(row, 4, 'date')
            newsheet.write(row, 5, 'year')
            newsheet.write(row, 6, 'month')
            newsheet.write(row, 7, 'represented_units')
            newsheet.write(row, 8, 'piped')
            newsheet.write(row, 9, 'well')
            newsheet.write(row, 10, 'surface')
            newsheet.write(row, 11, 'csf_run_no')
            newsheet.write(row, 12, 'piped_expenditure')
            newsheet.write(row, 13, 'surface_expenditure')
            newsheet.write(row, 14, 'expenditure')
            newsheet.write(row, 15, 'well_consumption')
            newsheet.write(row, 16, 'water_value_lost')
            newsheet.write(row, 17, 'well_expenditure')

        subdist_name = 'subdist_' + ind.name[9:15] + '_wrapper'
        gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate

        piped_consumption = ind.get_history('piped_consumption')[t_idx] *(365/12)
        well_consumption = ind.get_history('well_consumption')[t_idx] *(365/12)
        surface_consumption = ind.get_history('surface_consumption')[t_idx] * (365 / 12)

        newsheet.write(row+1, 0, ind.name)
        newsheet.write(row+1, 1, ind.name[9:15])
        newsheet.write(row+1, 2, gov_agent.gov_name)
        newsheet.write(row+1, 3, t_idx+1)
        newsheet.write(row+1, 4, t, date_format)
        newsheet.write(row+1, 5, t.year)
        newsheet.write(row+1, 6, t.month)
        newsheet.write(row+1, 7, ind.get_history('represented_units')[t_idx])
        try:
            newsheet.write(row+1, 8, piped_consumption)
        except TypeError:
            newsheet.write(row+1, 8, 'NA')
        try:
            newsheet.write(row+1, 9, well_consumption)
        except TypeError:
            newsheet.write(row+1, 9, 'NA')
        try:
            newsheet.write(row+1, 10, surface_consumption)
        except TypeError:
            newsheet.write(row+1, 10, 'NA')
        try:
            newsheet.write(row+1, 11, sys.argv[1])
        except TypeError:
            newsheet.write(row+1, 11, 'NA')
        try:
            newsheet.write(row + 1, 12, ind.get_history('piped_expenditure')[t_idx])
        except TypeError:
            newsheet.write(row + 1, 12, 'NA')
        try:
            newsheet.write(row + 1, 13, ind.get_history('surface_expenditure')[t_idx])
        except TypeError:
            newsheet.write(row + 1, 13, 'NA')
        try:
            newsheet.write(row + 1, 14, ind.get_history('expenditure')[t_idx])
        except TypeError:
            newsheet.write(row+1, 14, 'NA')
        try:
            newsheet.write(row + 1, 15, ind.get_history('well_consumption')[t_idx])
        except TypeError:
            newsheet.write(row+1, 15, 'NA')
        try:
            newsheet.write(row + 1, 16, ind.get_history('water_value_lost')[t_idx])
        except TypeError:
            newsheet.write(row+1, 16, 'NA')
        try:
            newsheet.write(row + 1, 17, ind.get_history('well_expenditure')[t_idx])
        except TypeError:
            newsheet.write(row + 1, 17, 'NA')


        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Industrial Agents')
csv_file = open('outputs\\industrial' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\groundwater' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Groundwater')
gw = s.network.get_institution('all_gw_nodes')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for g in gw.nodes:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'subdist')
            newsheet.write(row, 2, 'gov')
            newsheet.write(row, 3, 'aquifer')
            newsheet.write(row, 4, 'type')
            newsheet.write(row, 5, 'timestep')
            newsheet.write(row, 6, 'date')
            newsheet.write(row, 7, 'year')
            newsheet.write(row, 8, 'month')
            newsheet.write(row, 9, 'head')
            newsheet.write(row, 10, 'perc_sat')
            newsheet.write(row, 11, 'pumping')
            newsheet.write(row, 12, 'base_pumping')
            newsheet.write(row, 13, 'basin')
            newsheet.write(row, 14, 'csf_run_no')
            newsheet.write(row, 15, 'lift')
            newsheet.write(row, 16, 'capacity_reduction_factor')
            newsheet.write(row, 17, 'top')
            newsheet.write(row, 18, 'use_type')


        subdist_name = 'subdist_' + g.name[0:6] + '_wrapper'
        gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate
        perc_sat = (g.get_history('head_bias_corr')[t_idx] - g._gw_node_properties['bot']) / (g.get_history('head_bias_corr')[0] - g._gw_node_properties['bot'])

        newsheet.write(row+1, 0, g.name)
        newsheet.write(row+1, 1, g.name[0:6])
        newsheet.write(row+1, 2, gov_agent.gov_name)
        newsheet.write(row+1, 3, g.name[-2:])
        newsheet.write(row+1, 4, g.name[7:9])
        newsheet.write(row+1, 5, t_idx+1)
        newsheet.write(row+1, 6, t, date_format)
        newsheet.write(row+1, 7, t.year)
        newsheet.write(row+1, 8, t.month)
        newsheet.write(row+1, 9, g.get_history('head_bias_corr')[t_idx])
        newsheet.write(row+1, 10, perc_sat)
        newsheet.write(row+1, 11, g.get_history('pumping')[t_idx])
        newsheet.write(row+1, 12, g._gw_node_properties['baseline_pumping'])
        newsheet.write(row+1, 13, g._gw_node_properties['basin'])
        newsheet.write(row+1, 14, sys.argv[1])
        newsheet.write(row + 1, 15, g.get_history('lift')[t_idx])
        newsheet.write(row + 1, 16, g.get_history('capacity_reduction_factor')[t_idx])
        newsheet.write(row + 1, 17, g._gw_node_properties['top'])
        newsheet.write(row + 1, 18, g._gw_node_properties['use_type'])

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Groundwater')
csv_file = open('outputs\\groundwater' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\farmer' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Highland Farms')
hfarms = s.network.get_institution('all_highland_farms')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for hf in hfarms.nodes:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'subdist')
            newsheet.write(row, 2, 'gov')
            newsheet.write(row, 3, 'type')
            newsheet.write(row, 4, 'timestep')
            newsheet.write(row, 5, 'date')
            newsheet.write(row, 6, 'year')
            newsheet.write(row, 7, 'month')
            newsheet.write(row, 8, 'tanker_offer_quantity_initial')
            newsheet.write(row, 9, 'gw_sale_to_tanker')
            newsheet.write(row, 10, 'gw_sale_to_tanker_profits')
            newsheet.write(row, 11, 'ag_profit')
            newsheet.write(row, 12, 'irrigation supply')
            newsheet.write(row, 13, 'issp water use')
            newsheet.write(row, 14, 'well supply reduction')
            newsheet.write(row, 15, 'total_profit')
            newsheet.write(row, 16, 'crop_revenue_factor')
            newsheet.write(row, 17, 'ag_reduction_factor')
            newsheet.write(row, 18, 'issp_crop_revenue')
            newsheet.write(row, 19, 'csf_run_no')
            newsheet.write(row, 20, 'irrigation water')
            newsheet.write(row, 21, 'tanker offer quantity')

        subdist_name = 'subdist_' + hf.name[0:6] + '_wrapper'
        gov_agent = s.network.get_institution(subdist_name).hh_agents[0].governorate

        newsheet.write(row + 1, 0, hf.name)
        newsheet.write(row + 1, 1, hf.name[0:6])
        newsheet.write(row + 1, 2, gov_agent.gov_name)
        newsheet.write(row + 1, 3, hf.name[7:10])
        newsheet.write(row + 1, 4, t_idx + 1)
        newsheet.write(row + 1, 5, t, date_format)
        newsheet.write(row + 1, 6, t.year)
        newsheet.write(row + 1, 7, t.month)
        newsheet.write(row + 1, 8, hf.get_history('tanker_offer_quantity_initial')[t_idx])
        newsheet.write(row + 1, 9, hf.get_history('gw_sale_to_tanker')[t_idx])
        newsheet.write(row + 1, 10, hf.get_history('gw_sale_to_tanker_profits')[t_idx])
        newsheet.write(row + 1, 11, hf.get_history('cropping_profits')[t_idx])
        newsheet.write(row + 1, 15, hf.get_history('total_farmer_profits')[t_idx])
        newsheet.write(row+1, 19, sys.argv[1])
        newsheet.write(row+1,20,hf.get_history('irrig_allocation')[t_idx])
        newsheet.write(row + 1, 21, hf.get_history('tanker_offer_quantity')[t_idx])

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Highland Farms')
csv_file = open('outputs\\farmer' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\reservoir' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='Reservoirs')
res = s.network.get_institution('all_reservoir_nodes')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for r in res.nodes:
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'timestep')
            newsheet.write(row, 2, 'date')
            newsheet.write(row, 3, 'year')
            newsheet.write(row, 4, 'month')
            newsheet.write(row, 5, 'volume')
            newsheet.write(row, 6, 'evap')
            newsheet.write(row, 7, 'release')
            newsheet.write(row, 8, 'inflow')
            newsheet.write(row, 9, 'csf_run_no')

        volume = r.get_history('volume')[t_idx]
        evap = r.get_history('evap')[t_idx]
        release = r.get_history('release')[t_idx]
        inflow = r.get_history('inflow')[t_idx]

        if np.isnan(volume):
            volume = 'nan'
        if np.isnan(evap):
            evap = 'nan'
        if np.isnan(release):
            release = 'nan'
        if np.isnan(inflow):
            inflow = 'nan'

        if isinstance(inflow, str):
           pass
        else:
            if np.isinf(inflow):
                inflow = 'inf'

        newsheet.write(row + 1, 0, r.name)
        newsheet.write(row + 1, 1, t_idx + 1)
        newsheet.write(row + 1, 2, t, date_format)
        newsheet.write(row + 1, 3, t.year)
        newsheet.write(row + 1, 4, t.month)
        newsheet.write(row + 1, 5, volume)
        newsheet.write(row + 1, 6, evap)
        newsheet.write(row + 1, 7, release)
        newsheet.write(row + 1, 8, inflow)
        newsheet.write(row+1, 9, sys.argv[1])

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('Reservoirs')
csv_file = open('outputs\\reservoir' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\institution' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='WAJ')
waj = s.network.get_institution('waj')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for gov in waj.gov_delivery.keys():
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'timestep')
            newsheet.write(row, 2, 'date')
            newsheet.write(row, 3, 'year')
            newsheet.write(row, 4, 'month')
            newsheet.write(row, 5, 'type')
            newsheet.write(row, 6, 'value')
            newsheet.write(row, 7, 'population')
            newsheet.write(row, 8, 'csf_run_no')

        newsheet.write(row + 1, 0, gov)
        newsheet.write(row + 1, 1, t_idx + 1)
        newsheet.write(row + 1, 2, t, date_format)
        newsheet.write(row + 1, 3, t.year)
        newsheet.write(row + 1, 4, t.month)
        newsheet.write(row + 1, 5, 'gov delivery')
        newsheet.write(row + 1, 6, waj.get_history('gov_delivery')[t_idx][gov])
        newsheet.write(row + 1, 7, waj.get_history('population')[t_idx][gov])
        newsheet.write(row + 1, 8, sys.argv[1])

        row += 1
    for ext in waj.real_extraction.keys():
        newsheet.write(row + 1, 0, ext)
        newsheet.write(row + 1, 1, t_idx + 1)
        newsheet.write(row + 1, 2, t, date_format)
        newsheet.write(row + 1, 3, t.year)
        newsheet.write(row + 1, 4, t.month)
        newsheet.write(row + 1, 5, 'extraction')
        newsheet.write(row + 1, 6, waj.get_history('real_extraction')[t_idx][ext])
        newsheet.write(row + 1, 8, sys.argv[1])

        row += 1
    t_idx += 1

# EXCEL - Write JVA results for tableau
newsheet = newbook.add_worksheet(name='JVA')
jva = s.network.get_institution('jva')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for so in jva.node_delivery.keys():
        if so[0:2] == 'so':
            if row == 0:
                newsheet.write(row, 0, 'name')
                newsheet.write(row, 1, 'timestep')
                newsheet.write(row, 2, 'date')
                newsheet.write(row, 3, 'year')
                newsheet.write(row, 4, 'month')
                newsheet.write(row, 5, 'type')
                newsheet.write(row, 6, 'value')
                newsheet.write(row, 7, 'csf_run_no')

            newsheet.write(row + 1, 0, so)
            newsheet.write(row + 1, 1, t_idx + 1)
            newsheet.write(row + 1, 2, t, date_format)
            newsheet.write(row + 1, 3, t.year)
            newsheet.write(row + 1, 4, t.month)
            newsheet.write(row + 1, 5, 'so delivery')
            newsheet.write(row + 1, 6, jva.node_delivery_history[t_idx][so])
            newsheet.write(row + 1, 7, sys.argv[1])

            row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('WAJ')
csv_file = open('outputs\\institutionwaj' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()
sh = wb.sheet_by_name('JVA')
csv_file = open('outputs\\institutionjva' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\ww' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='WW')
wwtps = s.network.get_institution('all_wwtp_nodes')
row = 0
t_idx = 0
for t in s.network.timesteps:
    for wwtp in wwtps.nodes:
        irrigation = 0
        total_effluent = 0
        for dest in wwtp.effluent_dest:
            if dest == 'irrigation':
                irrigation += wwtp.effluent_history[t_idx][dest]
                total_effluent += wwtp.effluent_history[t_idx][dest]
            else:
                total_effluent += wwtp.effluent_history[t_idx][dest]
        if row == 0:
            newsheet.write(row, 0, 'name')
            newsheet.write(row, 1, 'timestep')
            newsheet.write(row, 2, 'date')
            newsheet.write(row, 3, 'year')
            newsheet.write(row, 4, 'month')
            newsheet.write(row, 5, 'ww_irrigation')
            newsheet.write(row, 6, 'ww_total')
            newsheet.write(row, 7, 'influent')
            newsheet.write(row, 8, 'csf_run_no')

        try:
            newsheet.write(row + 1, 0, wwtp.name)
        except TypeError:
            newsheet.write(row + 1, 0, 'NA')
        try:
            newsheet.write(row + 1, 1, t_idx + 1)
        except TypeError:
            newsheet.write(row + 1, 1, 'NA')
        try:
            newsheet.write(row + 1, 2, t, date_format)
        except TypeError:
            newsheet.write(row + 1, 2, t, 'NA')
        try:
            newsheet.write(row + 1, 3, t.year)
        except TypeError:
            newsheet.write(row + 1, 3, 'NA')
        try:
            newsheet.write(row + 1, 4, t.month)
        except TypeError:
            newsheet.write(row + 1, 4, 'NA')
        try:
            newsheet.write(row + 1, 5, irrigation)
        except TypeError:
            newsheet.write(row + 1, 5, 'NA')
        try:
            newsheet.write(row + 1, 6, total_effluent)
        except TypeError:
            newsheet.write(row + 1, 6, 'NA')
        try:
            newsheet.write(row + 1, 7, wwtp.get_history('influent')[t_idx])
        except TypeError:
            newsheet.write(row + 1, 7, 'NA')
        try:
            newsheet.write(row + 1, 8, sys.argv[1])
        except TypeError:
            newsheet.write(row + 1, 8, 'NA')

        row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('WW')
csv_file = open('outputs\\ww' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()

filename = 'outputs\\cwa_' + nowstring + '.xlsx'
newbook = xlsxwriter.Workbook(filename)
date_format = newbook.add_format({'num_format': 'mm/dd/yy'})

newsheet = newbook.add_worksheet(name='CWA')
cwa = s.network.get_institution('cwa')
row = 0
t_idx = 0
for t in s.network.timesteps:
    if row == 0:
        newsheet.write(row, 1, 'timestep')
        newsheet.write(row, 2, 'date')
        newsheet.write(row, 3, 'year')
        newsheet.write(row, 4, 'month')
        newsheet.write(row, 5, 'wehdah_release')
        newsheet.write(row, 6, 'tiberias_kac_actual')
        newsheet.write(row, 7, 'kac_zai_actual')
        newsheet.write(row, 8, 'extra_to_israel')
        newsheet.write(row, 9, 'concession_actual')
        newsheet.write(row, 10, 'alpha_kac_actual')
        newsheet.write(row, 11, 'allocation_actual')
        newsheet.write(row, 12, 'adasiya_syr_inflow')

    newsheet.write(row + 1, 1, t_idx + 1)
    newsheet.write(row + 1, 2, t, date_format)
    newsheet.write(row + 1, 3, t.year)
    newsheet.write(row + 1, 4, t.month)
    newsheet.write(row + 1, 5, cwa.get_history('wehdah_release')[t_idx])
    newsheet.write(row + 1, 6, cwa.get_history('tiberias_kac_actual')[t_idx])
    newsheet.write(row + 1, 7, cwa.get_history('kac_zai_actual')[t_idx])
    newsheet.write(row + 1, 8, cwa.get_history('extra_to_israel')[t_idx])
    newsheet.write(row + 1, 9, cwa.get_history('concession_actual')[t_idx])
    newsheet.write(row + 1, 10, cwa.get_history('alpha_kac_actual')[t_idx])
    newsheet.write(row + 1, 11, cwa.get_history('allocation_actual')[t_idx])
    newsheet.write(row + 1, 12, cwa.get_history('adasiya_syr_inflow')[t_idx])
    row += 1
    t_idx += 1

newbook.close()
wb = xlrd.open_workbook(filename)
sh = wb.sheet_by_name('CWA')
csv_file = open('outputs\\cwa' + nowstring + '.csv', 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
for rownum in xrange(sh.nrows):
    wr.writerow(sh.row_values(rownum))
csv_file.close()