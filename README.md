BiTemp
======

Playground for bi-temporal data.

# What is bi-temporal

A bi-temporal data repository stores data with two time dimensions.

One is te **know at** or *transaction* time.
The time at which the *fact* (the values of an entity) become *known* (present) in the system (repository).
This is more like a system or environmental time stamp.

The other is the **valid at** time. 
The time or time interval at which a *fact* is valid within the domain.
This is domain specific time stamp or time range (specific to the modeled reality) ant not related to the system or 
the environment.

# Bi-temporal implementation

There are two ways of time-stamping:
* tuple/entity time-stamping
* attribute time-stamping

Here we use tuple/entity time-stamping because:
* having two time dimension on each attribute is more overhead than per entity,
* it seems more logical (and understandable) to reason about bi-temporality (?) at the entity level than on the 
  attribute level.

An entity is a container for attributes, as such an entity with its attributes (values) is a logical unit and
should be stored (modified/updated/deleted) as a whole. 
