import cdsapi
import os
import xarray as xr
import pandas as pd
import requests
import logging
from windpowerlib import ModelChain, WindTurbine, create_power_curve

try:
    from matplotlib import pyplot as plt
except ImportError:
    plt = None

# Conectando à API do Copernicus
c = cdsapi.Client()

# Ponto específico (latitude, longitude)
latitude = -3.57168   # Exemplo
longitude = -38.84797  # Exemplo

# Solicitação dos dados do ERA5
result = c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': [
            '10m_u_component_of_wind', '10m_v_component_of_wind',
            '100m_u_component_of_wind', '100m_v_component_of_wind',
            '2m_temperature', 'temperature_100m',
            'surface_roughness', 'surface_pressure'
        ],
        'year': '2023',
        'month': '11',
        'day': '01',
        'time': '12:00',
        'format': 'netcdf',
        'area': [latitude, longitude, latitude, longitude],  # ponto específico
    }
)

# Baixar os dados em um arquivo local
result.download('era5_data.nc')

# Carregar o arquivo NetCDF
data = xr.open_dataset('era5_data.nc')

# Extraindo os dados em um DataFrame do Pandas
df = pd.DataFrame({
    'wind_speed_10m_u': data['u10'].values.flatten(),
    'wind_speed_10m_v': data['v10'].values.flatten(),
    'wind_speed_100m_u': data['u100'].values.flatten(),
    'wind_speed_100m_v': data['v100'].values.flatten(),
    'temperature_2m': data['t2m'].values.flatten(),
    'temperature_100m': data['t100'].values.flatten(),
    'surface_roughness': data['z0'].values.flatten(),
    'surface_pressure': data['sp'].values.flatten()
})

# Salvando o DataFrame em um arquivo CSV
df.to_csv('era5_data.csv', index=False)

def get_weather_data(filename="era5_data.csv", **kwargs):
    r"""
    Imports weather data from a file.

    The data include wind speed at two different heights in m/s, air
    temperature in two different heights in K, surface roughness length in m
    and air pressure in Pa. The height in m for which the data applies is
    specified in the second row.
    In case no weather data file exists, an example weather data file is
    automatically downloaded and stored in the same directory as this example.

    Parameters
    ----------
    filename : str
        Filename of the weather data file. Default: 'weather.csv'.

    Other Parameters
    ----------------
    datapath : str, optional
        Path where the weather data file is stored.
        Default is the same directory this example is stored in.

    Returns
    -------
    :pandas:`pandas.DataFrame<frame>`
        DataFrame with time series for wind speed `wind_speed` in m/s,
        temperature `temperature` in K, roughness length `roughness_length`
        in m, and pressure `pressure` in Pa.
        The columns of the DataFrame are a MultiIndex where the first level
        contains the variable name as string (e.g. 'wind_speed') and the
        second level contains the height as integer at which it applies
        (e.g. 10, if it was measured at a height of 10 m). The index is a
        DateTimeIndex.

    """

    if "datapath" not in kwargs:
        kwargs["datapath"] = os.path.dirname(__file__)

    file = os.path.join(kwargs["datapath"], filename)

    # download example weather data file in case it does not yet exist
    if not os.path.isfile(file):
        logging.debug("Download weather data for example.")
        req = requests.get("https://osf.io/59bqn/download")
        with open(file, "wb") as fout:
            fout.write(req.content)

    # read csv file
    weather_df = pd.read_csv(
        file,
        index_col=0,
        header=[0, 1],
    )
    weather_df.index = pd.to_datetime(weather_df.index, utc=True)

    # change time zone
    weather_df.index = weather_df.index.tz_convert("Europe/Berlin")

    return weather_df


def initialize_wind_turbines():
    r"""
    Initializes three :class:`~.wind_turbine.WindTurbine` objects.

    This function shows three ways to initialize a WindTurbine object. You can
    either use turbine data from the OpenEnergy Database (oedb) turbine library
    that is provided along with the windpowerlib, as done for the
    'enercon_e126', or specify your own turbine by directly providing a power
    (coefficient) curve, as done below for 'my_turbine', or provide your own
    turbine data in csv files, as done for 'my_turbine2'.

    To get a list of all wind turbines for which power and/or power coefficient
    curves are provided execute `
    `windpowerlib.wind_turbine.get_turbine_types()``.

    Returns
    -------
    Tuple (:class:`~.wind_turbine.WindTurbine`,
           :class:`~.wind_turbine.WindTurbine`,
           :class:`~.wind_turbine.WindTurbine`)

    """
    # ************************************************************************
    # **** Data is provided in the oedb turbine library **********************

    enercon_e126 = {
        "turbine_type": "E-126/4200",  # turbine type as in register
        "hub_height": 135,  # in m
    }
    e126 = WindTurbine(**enercon_e126)

    # ************************************************************************
    # **** Specification of wind turbine with your own data ******************
    # **** NOTE: power values and nominal power have to be in Watt


    # ************************************************************************
    # **** Specification of wind turbine with data in own file ***************

    # Read your turbine data from your data file using functions like
    # pandas.read_csv().
    # >>> import pandas as pd
    # >>> my_data = pd.read_csv("path/to/my/data/file")
    # >>> my_power = my_data["my_power"]
    # >>> my_wind_speed = my_data["my_wind_speed"]

    my_power = pd.Series(
        [0.0, 39000.0, 270000.0, 2250000.0, 4500000.0, 4500000.0]
    )
    my_wind_speed = (0.0, 3.0, 5.0, 10.0, 15.0, 25.0)

    return e126


def calculate_power_output(weather, e126):
    r"""
    Calculates power output of wind turbines using the
    :class:`~.modelchain.ModelChain`.

    The :class:`~.modelchain.ModelChain` is a class that provides all necessary
    steps to calculate the power output of a wind turbine. 

    Parameters
    ----------
    weather : :pandas:`pandas.DataFrame<frame>`
        Contains weather data time series.
    e126 : :class:`~.wind_turbine.WindTurbine`
        WindTurbine object with power curve from the OpenEnergy Database
        turbine library.

    """

    # ************************************************************************
    # **** ModelChain with non-default specifications ************************
    modelchain_data = {
        "wind_speed_model": "logarithmic",  # 'logarithmic' (default),
        # 'hellman' or
        # 'interpolation_extrapolation'
        "density_model": "ideal_gas",  # 'barometric' (default), 'ideal_gas' or
        # 'interpolation_extrapolation'
        "temperature_model": "linear_gradient",  # 'linear_gradient' (def.) or
        # 'interpolation_extrapolation'
        "power_output_model": "power_coefficient_curve",  # 'power_curve'
        # (default) or 'power_coefficient_curve'
        "density_correction": True,  # False (default) or True
        "obstacle_height": 0,  # default: 0
        "hellman_exp": None,
    }  # None (default) or None
    # initialize ModelChain with own specifications and use run_model method
    # to calculate power output
    mc_e126 = ModelChain(e126, **modelchain_data).run_model(weather)
    # write power output time series to WindTurbine object
    e126.power_output = mc_e126.power_output

    return


def plot_or_print(e126):
    r"""
    Plots or prints power output and power (coefficient) curves.

    Parameters
    ----------
    e126 : :class:`~.wind_turbine.WindTurbine`
        WindTurbine object with power curve from the OpenEnergy Database
        turbine library.
    """

    # plot or print turbine power output
    if plt:
        e126.power_output.plot(legend=True, label="Enercon E126")
        plt.xlabel("Time")
        plt.ylabel("Power in W")
        plt.show()
    else:
        print(e126.power_output)
       
    # plot or print power curve
    if plt:
        if e126.power_curve is not False:
            e126.power_curve.plot(
                x="wind_speed",
                y="value",
                style="*",
                title="Enercon E126 power curve",
            )
            plt.xlabel("Wind speed in m/s")
            plt.ylabel("Power coefficient $\mathrm{C}_\mathrm{P}$")
            plt.show()
      


def run_example():
    r"""
    Runs the basic example.

    """
    # You can use the logging package to get logging messages from the
    # windpowerlib. Change the logging level if you want more or less messages:
    # logging.DEBUG -> many messages
    # logging.INFO -> few messages
    logging.getLogger().setLevel(logging.DEBUG)

    weather = get_weather_data("weather.csv")
    my_turbine, e126, my_turbine2 = initialize_wind_turbines()
    calculate_power_output(weather, my_turbine, e126, my_turbine2)
    plot_or_print(my_turbine, e126, my_turbine2)


if __name__ == "__main__":
    run_example()
