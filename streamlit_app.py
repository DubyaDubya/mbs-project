import streamlit as st
import pyarrow as pa
import pandas as pd

from charts import plot1

st.title('How the Federal Reserve Took from the Young and Gave to the Old')


beginning_p = """The purpose of this report is to provide an exploratory analysis on the Federal Reserve's impact on
the Mortgage market through its covid-era policy of Quantitative Easing and its ongoing post-covid 
policy of Quantitative Tightening. The report uses data from the Federal Reserve, Fannie Mae and
Freddie Mac to show a link between The Federal Reserve's purchases and the characteristics of
mortgage borrowers. The report finds that the Fed's policy ultimately landed cash into the hands
of pre-existing home-owners, and it suggests that the housing market may suffer from significant
long term demand destruction due to affordability issues."""

a = """As an immediate response to covid, the Federal Reserve undertook a massive, unprecedented 
campaign to support the United States' economy through purchases of financial assets.
This is a monetary-policy tool known as Quantitative Easing or QE, and the idea was to 
stimulate spending (demand) in the economy through the introduction of new money.

Unsurprisingly, this worked b"""

st.write(beginning_p)
st.plotly_chart(plot1)