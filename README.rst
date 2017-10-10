=======================
Pipline Data Processing
=======================

Why?
----
Many tasks in machine learning, deep learning and other fields require complex data processing that takes a lot of time. Ideally, this processing should run in parallel to the main process, preparing data for usage (by neural net, for instance). PDP provide simple interface to organize pipeline of data processing with simple blocks that satisfy most typical needs.

Use cases
--------------
* Neural Net training, where you need a way to train net, load data from the disk and augment it. PDP allows user to do all these things at the same time without need to use *threading* or *multiprocessing* python modules directly.

Examples
--------
Are in repository in *examples* folder

Is it fast? 
-----------
Speed and parallel execution is a top priority. Right now threads are used to exchange information between pipline stages, because it's memory and CPU efficient to exchange data between threads and not processes. Python's threads are flawed by GIL, but it doesn't effect performance for IO-bound tasks and for numpy operations. Since all operations for data augmentations are likely to be done in numpy operations, performance will not be significantly affected by GIL.
