=============
Configuration
=============

This chapter explains how to configure Peekaboo.


Setup Directories and Users
===========================
We assume that the user you run Peekaboo with is ``peekaboo``.
Its home directory will be used by Peekaboo to store transient data.
``/var/lib/peekaboo`` is a good choice for this path and the default:

.. code-block:: shell

    $ groupadd -g 150 peekaboo
    $ useradd -g 150 -u 150 -m -d /var/lib/peekaboo peekaboo

If you plan to use AMaViSd to analyse email attachments with Peekaboo,
the Peekaboo user must be a member of the ``amavis`` group in order to access
the files from an email created by AMaViSd:

.. code-block:: shell

    $ gpasswd -a peekaboo amavis

**Note**: Extending the supplementary group list of the ``peekaboo`` user is
the preferred way to achieve access to submitted files.
A separate group can be employed for that if ``peekaboo`` would otherwise gain
access to more files than necessary and the client application supports
changing file ownership to that group before submit.
Alternatively, option ``group`` of ``peekaboo.conf`` can be used to affect the
group Peekaboo changes to when started as root.

In order for clients to submit files to Peekaboo, you will need to open its unix
domain socket up for connections by users other than ``peekaboo``.
The preferred way for that is to create a separate group, make all users who
will be submitting files to Peekaboo members of that group and then configure
the Peekaboo daemon to change group ownership of its socket to that group upon
startup using option ``socket_group``. Note that the ``peekaboo`` user itself
needs to be a member of that group in order to be allowed to change ownership
of its socket to it.

.. code-block:: shell

    $ groupadd -g 151 peekaboo-clients
    $ gpasswd -a peekaboo peekaboo-clients
    $ gpasswd -a amavis peekaboo-clients

If there's only a single client, i.e. amavis, the socket owner group can also
just be its primary group, of which ``peekaboo`` may already be a member in
order to access submitted files.

It is also possible, although not recommended, to open up the socket to all
users on the system by adjusting option ``socket_mode``.

You may choose VirtualBox as hypervisor. If so, you must add the Peekaboo user to the
``vboxusers`` group.

.. code-block:: shell

    $ sudo usermod -a -G vboxusers peekaboo


Configuration File
==================
Peekaboo requires a configuration file to be supplied on startup.
If no configuration file is supplied on startup, Peekaboo will look for a file
named ``/opt/peekaboo/etc/peekaboo.conf``. For details, please run

.. code-block:: shell

    $ /opt/peekaboo/bin/peekaboo --help

You can configure Peekaboo according to the sample configuration in
``/opt/peekaboo/share/doc/peekaboo/peekaboo.conf.sample`` and save it
to ``/opt/peekaboo/etc/peekaboo.conf``.
The same directory also contains a sample ``ruleset.conf``.


Database Configuration
======================
Peekaboo supports multiple databases. We did tests with SQLite, MySQL, and PostgreSQL.
However, Peekaboo should also work with other databases. For a full list of supported
database management systems, please visit the website of the 3rd party module *SQLAlchemy*.

MySQL
-----

.. code-block:: shell

    $ mysql -u root -p

.. code-block:: sql
   
   mysql> CREATE USER 'peekaboo'@localhost IDENTIFIED BY 'password';
   mysql> CREATE DATABASE peekaboo;
   mysql> GRANT ALL PRIVILEGES ON peekaboo.* TO 'peekaboo'@localhost;
   mysql> FLUSH PRIVILEGES;
   mysql> exit


PostgreSQL
----------

.. code-block:: shell
   
   $ sudo -u postgres psql postgres
   \password postgres

Crate User
++++++++++
   
.. code-block:: shell

    $ sudo -u postgres createuser peekaboo --encrypted --login --host=localhost --pwprompt

Create Database
+++++++++++++++

.. code-block:: shell

    $ sudo -u postgres createdb peekaboo --host=localhost --encoding=UTF-8 --owner=peekaboo


``systemd``
===========
Simply copy ``systemd/peekaboo.service`` to ``/etc/systemd/system/peekaboo.service``.
If you don't use the system's Python interpreter (``/usr/bin/python``) and have placed the configuration file
in ``/opt/peekaboo/etc/peekaboo.conf``, no changes to this file are required.

Finally, run ``systemctl daemon-reload``, so ``systemd`` recognizes Peekaboo.


Helpers & 3rd Party Applications
================================
Also, Peekaboo can run behavioural analysis of file and directories by utilizing Cuckoo sandbox for this purpose.
Further, email attachments can be supplied to Peekaboo for analysis directly from AMaViSd.

The remaining sections cover the setup of these components.

Cuckoo
------
Please refer to the Cuckoo documentation available at https://cuckoo.sh/docs/index.html.

AMaViSd
-------
First, install the ``10-ask_peekaboo`` plugin as
``/etc/amavis/conf.d/10-ask_peekaboo``.
It is available from the ``amavis`` subdirectory of the PeekabooAV installation
and has been tested with AMaViS 2.11.0.


Put the following code into ``/etc/amavis/conf.d/15-av_scanners``:

.. code-block:: perl

    @av_scanners = (
        ['Peekaboo-Analysis',
        \&ask_peekaboo, ["{}\n", "/var/run/peekaboo/peekaboo.sock"],
        qr/has been categorized "(unknown|checked|good|ignored)"$/m,
        qr/has been categorized "bad"$/m ],
    );

    1;  # ensure a defined return


Now change ``/etc/amavis/conf.d/15-content_filter_mode`` to:

.. code-block:: perl

    @bypass_virus_checks_maps = (
        \%bypass_virus_checks, \@bypass_virus_checks_acl, \$bypass_virus_checks_re);


and for mail notifications for the user ``peekaboo`` add this line to

``/etc/amavis/conf.d/25-amavis_helpers``:

.. code-block:: perl
   
   $virus_admin = 'peekaboo';

Next, create an ``/etc/amavis/conf.d/50-peekaboo`` and fill it with:

.. code-block:: perl
   
   # force a fresh child for each request
   $max_requests = 1;

   # if not autodetectable or misconfigured, override hostname and domain
   $mydomain = 'peekaboo.test';
   $myhostname = 'host.peekaboo.test';

   # Optional for development if you want to receive the results of AMaViSd via email
   $notify_method = 'smtp:[127.0.0.1]:10025';
   $forward_method = 'smtp:[127.0.0.1]:10025';

Finally, restart AMaViSd

.. code-block:: shell

    systemctl restart amavis


Postfix
-------

In order to make Postifx forward emails to AMaViSd edit ``/etc/postfix/main.cf``:

.. code-block:: none
   
   $myhostname = 'host.peekaboo.test'
   $mydomain = 'peekaboo.test'
   
   content_filter=smtp-amavis:[127.0.0.1]:10024 
