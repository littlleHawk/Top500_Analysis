###################################################
#
#                 PROJECT MODULE
#     Contains 3rd party and built-in libraries
#
###################################################

# Import 3rd party and built-in libraries and specific methods
import warnings
import math
import pandas as pd
import numpy as np
import hashlib as hl
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import ipywidgets as widgets
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.ticker as plticker
import seaborn as sns
import scipy.stats as scipy
import re as re

from plotly.subplots import make_subplots
from ipywidgets import interact, Layout, VBox
from IPython.display import display
from scipy.stats import zscore, linregress, f_oneway
from scipy.optimize import curve_fit
from typing import List, Tuple, cast
from rapidfuzz import process, fuzz

# Configure settings
pio.renderers.default = 'notebook'
warnings.filterwarnings('ignore')

# set hash engine
hasher = hl.md5()

# Global Constants
VERBOSE_VAL = False