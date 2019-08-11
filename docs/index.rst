.. pymongo_import documentation master file, created by
   sphinx-quickstart on Sun Aug 20 15:41:21 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

========================================================
pymongoimport - Import csv files into MongoDB
========================================================

``pymongoimport`` is a collection of python programs for importing CSV
files into `MongoDB <http://mongodb.com/>`_.

 
Why do we have ``pymongoimport``?

MongoDB already has a perfectly good (and much faster)
`mongoimport <https://docs.mongodb.com/manual/reference/program/mongoimport/>`_ program 
that is available for free in the standard MongoDB `community download <https://www.mongodb.com/download-center#community>`_.

Well ``pymonogoimport`` does a few things that ``mongoimport`` doesn't do (yet). For people
with new CSV files there is the ``--genfieldfile`` option which will automatically
generate a typed field file for the specified input file. Even with a field file ``pymongoimport``
will fall back to the string type if type conversion fails on any input column.

``pymongoimport`` allows you to use the ``--addlocator``

pymongoimport also has the ability to restart  an upload from the
point where is finished. This restart capability is recorded in an
``audit`` collection in the current database. An audit record is
stored for each upload in progress and each completed upload. Thus the
audit collection gives you a record of all uploads by filename and
date time.

Finally pymongoimport is more forgiving of *dirty* data. So if your
actual data doesn't match your field type definitions then the type
converter will fall back to using a string type.

On the other hand
`mongoimport <https://docs.mongodb.com/manual/reference/program/mongoimport/>`_
supports the more extensive security options of the 
`MongoDB Enterprise Advanced <https://www.mongodb.com/products/mongodb-enterprise-advanced>`_
product and because it is written in `Go <https://golang.org/>`_ it can use threads more effectively and so is generally faster.


.. toctree::
   :maxdepth: 2
   :caption: pymongoimport command-line programs:
	     
   pymongoimport
   pymultiimport
   splitfile
   pwc


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
