# Field Descriptions

| Field                          | Description |
| ------------------------------ | ----------- |
| metadata_generatedAt           |             |
| metadata_recordType            |             |
| metadata_serialId_streamId     |             |
| metadata_serialId_bundleSize   |             |
| metadata_serialId_bundleId     |             |
| metadata_serialId_recordId     |             |
| metadata_serialId_serialNumber |             |
| metadata_receivedAt            |             |
| coreData_id                    |             |
| coreData_position_lat          |             |
| coreData_position_long         |             |
| coreData_elevation             |             |
| coreData_accelset_accelYaw     |             |
| coreData_speed                 |             |
| coreData_heading               |             |
| x_pos                          |             |
| y_pos                          |             |
------------------------------------------------

ASN.1 Representation:
    BSMcoreData ::= SEQUENCE {

    msgCnt MsgCount,
    id TemporaryID,
    secMark DSecond,
    lat Latitude,
    long Longitude,
    elev Elevation,
    accuracy PositionalAccuracy,
    transmission TransmissionState,
    speed Speed,
    heading Heading,
    angle SteeringWheelAngle,
    accelSet AccelerationSet4Way,
    brakes BrakeSystemStatus,
    size VehicleSize
}

MsgCount ::= INTEGER (0..127)

Use: The DE_MsgCount data element is used to provide a sequence number within a stream of messages with the same
DSRCmsgID and from the same sender. A sender may initialize this element to any value in the range of zero to 127 when
sending the first message with a given DSRCmsgID, or if the sender has changed identity (e.g., by changing its
TemporaryID) since sending the most recent message with that DSRCmsgID. Depending on the application the sequence
number may change with every message or may remain fixed during a stream of messages when the content within each
message has not changed from the prior message sent. For this element, the value after 127 is zero.

7.199 Data Element: DE_TemporaryID
Use: This is the 4 octet random device identifier, called the TemporaryID. When used for a mobile OBU device, this value
will change periodically to ensure the overall anonymity of the vehicle, unlike a typical wireless or wired 802 device ID.
Because this value is used as a means to identify the local vehicles that are interacting during an encounter, it is used in
the message set. Other devices, such as infrastructure (RSUs), may have a fixed value for the temporary ID value. See
also DE_StationID which is used in other deployment regions.
ASN.1 Representation:
TemporaryID ::= OCTET STRING (SIZE(4))
