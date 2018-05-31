# picoDAQ

*python* Data AcQuisition and analysis with PicoScope usb-oscilloscopes

The *usb*-oscilloscope series PicoSpope by Pico Technology 
(see <https://www.picotech.com>)
offers universal instruments that come with great software support, not
only a graphical interface offering the functionality known from oscilloscopes, 
but - most importantly for this project - also with a software development kit
(*SDK*) which makes it possible to use the devices with a wide range of
high-level languages. 

Provided here is a data acquisition system as is needed to record, 
analyze, classify and count the occurrence of wave forms such as provided 
for example by single-photon counters or typical detectors common in 
quantum mechanical measurements or in nuclear, particle physics and astro
particle physics, e.g. photo tubes, Geiger counters, avalanche photo-diodes
or modern SiPMs.

The random nature of such processes and the need to keep read-out dead 
times low requires an input buffer and a buffer manager running as a 
background process. Data are provided via the buffer manager 
interface to several consumer processes to analyze, check or visualize 
data and analysis results. Such consumers may be obligatory ones, 
i. e. data acquisition pauses if all input buffers are full and an 
obligatory consumer is still busy processing. A second type of random 
consumers receives an event copy from the buffer manager upon request, 
without pausing the data acquisition process. Typical examples of 
random consumers are displays of a subset of the wave forms or of 
intermediate analysis results. Examples for such consumers are provided
as part of this package, running either as a 'thread' within the python
interpreter or as sub-processes using the 'multiprocessing' package.

This project originated as a demonstration to analyze pulses from a 
photomultiplier (PM) or a Silicon Photo Multiplier (SiPM) registering
optical signals from  a detector, in the simplest case a coffeepot
filled with water and equipped with a PM to count muons from cosmic rays. 

## List of implemented **Functions**:

   class *picoConfig*:

   - set up PicoScope time base, channel ranges and trigger
   - set up the internal signal generator
   - PicoScope configuration read from *json* or *yaml* file
   - data acquisition of raw data from device

  class *BufferMan*:

   - acquire data (implemented as background thread)
   - manage event data buffer and distribute to consumers
   - configuration read from *json* or *yaml* file

    supports 
    
      - *obligatory* consumers: data acquisition pauses until consumer done

      - *random* consumers: receive a copy of one event, data acquisition 
      continues

  module *AnimatedInstruments* (deprecated, to be removed soon)

   - examples of animated graphical devices: a Buffer Manager display
        (using class *plotBufManInfo), a VoltMeter (class *VoltMeter*),
         an Oscilloscope (class *Ocscilloscope* and a ratemeter
         (class *RMeter*). The module must run as a *python* *thread* in
         the same *python* interpreter as *BufferMan*

 
  module *mpBufManCntrl*

   - this sub-process receives status and logging information from the Buffer
     Manager via a multiprocessing Queue and displays input rate history, filling
     level of the buffers and the data-acquisition lifetime. Buttons at the bottom
     of the window allow status changes (from RUNNING to PAUSED or vice versa) or to
     exit. A log-file at the end contains a summary and, optionally, logging
     information. 

  module *mpOsci*

   - runs an instance of *Oscilloscpe* as a sub-process, and receives
        data from *BufferMan* via a multiprocessing Queue.

  module *mpRMeter* 

   - runs an instance of the *RMeter* class as a sub-process, receiving
        via a multiprocessing Queue.

  module *mpVMeter* 

   - runs an instance of the *VoltMeter* class as a sub-process, receiving
        data via a multiprocessing Queue.

  module *mpHists* 
 
  - runs an instance of the *animHists* class as a sub-process; receives 
       input data via a multiprocessing Queue. Data are formatted as lists 
       of values. A normalized frequency distribution is then updated and 
       displayed.

  module *mpBDisplay* 

  - runs an instance of class BarDisplay and shows one (signed or unsigned)
       value per Channel (e.g. peak Voltage, effective Voltage etc.). Values 
       are passed to the sub-process via a multiprocessing Queue.

The script `runDAQ.py` gives an example of how to use all of the above. For a
full demo, connect the output of a PicoScope's signal generator to channel *B*,
and eventually an open cable to Channel *A* to see random noise. 
Use the configuration file `DAQconfig.json`, which specifies the configuration files `Mconfig.json` for the Buffer Manager and `PSConfig.json` for the
PicoScope. As a hook for own extensions, user code may be included. An example for this is shown in the configuration file `DAQ_Cosmo.json`, which points to a code snippet *anaDAQ.py* to starts some example consumers (code in
`exampleConsumers.py`).

## Examples

The directory `examples/` contains configuration files and some 
special applications. 

The script `runCosmo.py` depends on `pulseFilter.py`, which implements a
convolution filter to search for characteristic signal shapes in an input
waveform. The example is tailored to identify short pulses from muon detectors (the scintillator panels of the *CosMO*-experiment by
"Netzwerk Teilchenwelt", <http://www.teilchenwelt.de>, or the
Kamiokanne-Experiment with photomultiplier readout and pulses shaped to a length of approx. 150ns). A more complete and updated example has been moved to the project `picoCosmo`,
see <https://github.com/GuenterQuast/picoCosmo>. 

`runDataLogger.py` implements a data logger for slow signals rates below
0.1 Hz. Signals are sampled with a PicoSocpe at a rate of 100 KHz over
20 ms and then averaged. 50 Hz noise is thus eliminated, and a clean
Voltage signal obtained. The History of the recorded Voltages is
displayed using the module `mpDataLogger.py`.

Similarly, `runVoltMeter` uses a PicoScope device to display the
 effective voltage as bar graph and history plot.

## Installation of the package

This python code is compatible with *python* versions 2.7 and 3.5.
It was tested with PicoScope device classes PS2000, PS2000a,
PS3000a and PS4000 under Ubuntu, openSUSE Leap and on RaspberryPi.
Graphical displays are implemented with `matplotlib`.

**Requirements:**

  - The low-level drivers and C-libraries contained in the Pico Technology
    Software Development Kit are required,  *SDK* by Pico Technology,
    see  https://www.picotech.com/downloads
  - *python* bindings of the *pico-python* project by Colin O'Flynn
    and Mark Harfouche, https://github.com/colinoflynn/pico-python

*picoDAQ* presently consists of the modules in the direcoctry *picodaqa*, mentioned
above, and an example *python* script (*runDAQ.py*) with configuration examples
(*.yaml* files) for the data acquisition (*DAQconfig.yaml*), for the PicoScope
Device (*PSconfig.yaml*) and for the Buffer Mananger (*BMconfig.yaml*).

After downloading all files from the git repository, connect your PicoScope
and start from the command line, e. g. `python runDAQ.py`. 

You may run the script *make_dist.sh* to generate a *.whl* file in the
subdirectory *dist*, which can be installed via `pip install
picodaqa_<vers>_<type>.whl`. Once this is done, the provided examples can
be copied to any directory. 

