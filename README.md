Swift-Utilization
------

Install
-------

1) Install Swift-Utilization with ``sudo python setup.py install`` or ``sudo python
   setup.py develop`` or via whatever packaging system you may be using.

2) Alter your proxy-server.conf pipeline to have swiftutilization:

    [pipeline:main]
        pipeline = catch_errors cache swift3_gatekeeper
        swift3 s3token authtoken keystone swiftutilization proxy-server

    ! swift utilization middleware always located after auth middleware

3) Add to your proxy-server.conf the section for the swift utilization WSGI filter::

    [filter:swiftutilization]
    use = egg:swiftutilization#swiftutilization