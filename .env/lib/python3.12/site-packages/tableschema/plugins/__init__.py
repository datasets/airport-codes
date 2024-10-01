# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..helpers import PluginImporter


# Register importer
importer = PluginImporter(
    virtual='tableschema.plugins.', actual='tableschema_')
importer.register()

# Delete variables
del PluginImporter
del importer
