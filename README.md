# Analyse de l'impact carbone des supercalculs

Un tableau de bord interactif basé sur Python pour analyser la consommation d'énergie et les émissions de carbone des supercalculateurs inclus dans les listes Top500 et Green500 depuis 2012. Le tableau de bord compare les tendances nationales et mondiales en utilisant les données de Top500, Green500, Ember et Electricity Maps.

---

## Features
* Dashboard interactif (`Python3.8` jupyter notebook) avec 'ipywidgets' oú on peut comparer le consommation energetic de les CPU/GPUs dans les supercalculateurs
* Construction des ensembles des données avec information sur les puces, facteur d'impact carbone, et taux d'energy utilisation par chaque supercalculateur.

## Fichiers
* `Dashboard.ipynb` - Fichier principal avec le dashboard interactif
* `project_lib.py` - libraries utilisées par le projet
* `utilities.py` - base fonctions à lire et prétraite les données et fonctions utiles pour calculations
* `build_dfs.py` - leér données et creér datasets à utiliser dans l'analyse
* `dataset_counstructor_functions.py` - creer energie et carbon impact datasets à utiliser dans widget_functions
* `widgets.py`- initialize widgets pour le Dashboard
* `widget_functions.py` - creer les graphiques et text à montrer sur le Dashboard 
* `testbed.py` - fichier à jouer avex les données

## Données
### Listes de supercalculateurs
- **[Top500](https://top500.org/lists/top500/)** — Fichiers contenant chaque classement des 500 supercalculateurs les plus puissants, publiés de juin 1994 à novembre 2024.  
  Les fichiers sont nommés selon le format : `TOP500_YYYYMM.xls`.
- **[Green500](https://top500.org/lists/green500/)** — Fichiers contenant chaque classement des 500 supercalculateurs les plus efficaces sur le plan énergétique, publiés de juin 2016 à novembre 2024.  
  Les fichiers sont nommés selon le format : `green500_YYYY_MM.xlsx`.

### Données d’impact carbone de l’électricité
- **[Ember](https://ember-energy.org/data/monthly-electricity-data/)** — Données mensuelles sur la production d’électricité et les émissions de gaz à effet de serre par pays. Dans le fichier `monthly_full_release_long_format.csv`

- **[Electricity Maps](https://portal.electricitymaps.com/datasets)** — Données mensuelles sur l’intensité carbone directe et en cycle de vie pour les années 2021 à 2024.
  Les fichiers sont nommés selon le format : `XX_YYY_monthly.csv`
  * `ElectricityMaps_coutry_abbr_list.csv` est utilisé pour les lire

### Puces
- `GPU_TDP` - Donnée sur le TDP, fequency, et nombre des cores des CPUs utilisées par les supercalculaters
- `CPU_TDP` - Donnée sur le TDP et nombre de shaders (cores) des GPUs utilisées par les supercalculateurs

### DataFrames utilisées
**connect_cores_computers_electricity** - données du puces, carbone impact d'electricity, et données de Top500 ou Green500 pour chaque supercalculateur
**chip_df** - DataFrame avec le données des supercalculateurs et leurs puces
**electricity_impact_df** - données de Ember et ElectricityMaps


## Glossary
| Terme                         | Définition |
|-------------------------------|------------|
| **TDP (Thermal Design Power)**| Puissance thermique maximale qu’un composant (comme un CPU ou GPU) est censé dissiper dans des conditions normales ; utilisée ici pour estimer la consommation électrique. |
| **CPU / GPU**                 | Unité Centrale de Traitement (CPU) exécute les calculs généraux. Le Processeur Graphique (GPU) est optimisé pour les calculs parallèles et graphiques. |
| **Cycle de vie**              | Approche qui prend en compte l’ensemble des émissions de CO₂ générées depuis la production jusqu’à l’élimination (et pas seulement à l’usage). |
| **Intensité carbone**         | Quantité de CO₂ émise par kilowattheure (gCO₂/kWh) d’électricité produite ; varie selon le pays et la source d’énergie. |
| **Impact carbone**            | Estimation des émissions de CO₂ générées par un supercalculateur selon sa consommation d’énergie et l’intensité carbone du pays hôte. |
| **Top500 / Green500**         | Classement des supercalculateurs les plus puissants / le plus effacace en termes d'énergie au monde |
| **RMax**                      | Performance maximale theoretique |
| **RPeak**                     | Performance maximale atteninte sur le [Linpack Benchmark]('https://www.top500.org/project/linpack/') (GFLOPS) |
| **Accelerators**              | GPUs ou autre puces spécialisées utilisées pour le calcul |
| **Power Efficiency**          | Performance par Watt (MFLOPS/W) maximale |
| **Idle Power**                | Énergie utilisée par un système au 'idle' |


## Sources
Code et de données de:
* https://top500.org/
* https://www.techpowerup.com/gpu-specs/
* https://www.techpowerup.com/cpu-specs/
* https://ember-energy.org/
* https://portal.electricitymaps.com/datasets
* https://github.com/abenhari/green500-analysis
* https://github.com/Boavizta/boaviztapi/blob/main/README.md

## Authors and acknowledgment
Ce projet été realisée grâce à l'equipe NeS à LaBRI et le direction de Aurélie Bugeau, Gaël Guennebaud, et Anne Vialard