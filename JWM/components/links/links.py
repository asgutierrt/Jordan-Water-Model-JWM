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

from pynsim import Link

class JordanLink(Link):
    """The Jordan link class.

    The Jordan link class is a parent class that provides common methods for links in the Jordan model.

    **Properties**:

        |  *institution_names* (list) - list of institutions associated with link

     Edit: linktype property added to run the waj code. Only solution the WAJ engine would allow transfers, reason unknown. Do not merge. Not ideal: types themselves decided by user.
     probably a better method with introspection.
    """

    description = "Common Methods for Jordan Links"

    _properties = dict(
        linktype=None,
        maxflow=None,
        flow=None,
    )

    def __init__(self, name, **kwargs):
        super(JordanLink, self).__init__(name, **kwargs)
        self.institution_names = []

    def add_to_institutions(self, institution_list, n):
        """Add link to institutions.

        Adds link to each institution in a list of Jordan Institution objects.

        **Arguments**:

        |  *institution_list* (list) - list of institutions associated with link
        |  *n* (pynsim network) - network

        """

        for institution in n.institutions:
            for inst_name in institution_list:
                if institution.name.lower() == inst_name.lower():
                    institution.add_link(self)


class River(JordanLink):
    pass

class GW_Pipeline(JordanLink):
    """
    separated from the pipeline class as it is a more virtual element
    """
    pass

class Pipeline(JordanLink):

    description = 'pipeline properties (temporary edition-thibaut)'

    _properties = dict(
        diameter=None,
        multiplier=None,
        Vmax=None,
        max_flow=None,
        cost=None,
        flow=None
    )

    def det_capacity(self):
        capacity = self.Vmax*3600*24*31*3.1416*self.diameter**2/4*self.multiplier
        return capacity


class KAC(JordanLink):
    pass


class Return(JordanLink):
    pass


class Nearest(JordanLink):
    pass