import os
from unittest.mock import MagicMock, patch
import pytest
from model import GRINState, GRINStatus, Record, RecordState, FRBRStatus
from processes.grin.conversion import GRINConversion
from datetime import datetime
from uuid import uuid4
import json



@pytest.fixture(scope="function")
def setup_barcodes_conversion(db_manager):
    hardcoded_barcodes_list = [
        "3433124920780",
        "3433124920797",
        "3433124920803",
        "3433124920810",
        "3433124920827"
    ]

    initial_state_map = {
       barcode: GRINState.PENDING_CONVERSION.value for barcode in hardcoded_barcodes_list
    }

    hardcoded_record_id_map = {
        "3433124920780": 8888001,
        "3433124920797": 8888002,
        "3433124920803": 8888003,
        "3433124920810": 8888004,
        "3433124920827": 8888005
    }
    
    for barcode_str in hardcoded_barcodes_list:
        existing_status_record = db_manager.session.query(GRINStatus).filter_by(barcode=barcode_str).first()
        if existing_status_record:
            if existing_status_record.record_id:
                existing_main_record = db_manager.session.query(Record).filter_by(id=existing_status_record.record_id).first()
                if existing_main_record:
                    db_manager.session.delete(existing_main_record)
            db_manager.session.delete(existing_status_record)
            db_manager.session.commit()

        record_id_for_this_barcode = hardcoded_record_id_map[barcode_str]
        record_uuid_for_record_table = str(uuid4()) 
        main_record = Record(
            id=record_id_for_this_barcode,
            uuid=record_uuid_for_record_table,
            title=f"Test Record for Barcode {barcode_str}",
            source="test_fixture",
            source_id=f"{barcode_str}|grin",
            state=RecordState.STAGED.value,
            frbr_status=FRBRStatus.TODO.value
        )
        db_manager.session.add(main_record)

        status_record = GRINStatus(
            barcode=barcode_str,
            state=initial_state_map[barcode_str],
            record_id=record_id_for_this_barcode,
            failed_download=0
        )
        db_manager.session.add(status_record)
    
    db_manager.session.commit()

    yield hardcoded_barcodes_list

    for barcode_to_delete in hardcoded_barcodes_list:
        status_record_to_delete = db_manager.session.query(GRINStatus).filter_by(barcode=barcode_to_delete).first()
        if status_record_to_delete:
            record_id_to_delete = status_record_to_delete.record_id
            db_manager.session.delete(status_record_to_delete)
            if record_id_to_delete:
                main_record_to_delete = db_manager.session.query(Record).filter_by(id=record_id_to_delete).first()
                if main_record_to_delete:
                    db_manager.session.delete(main_record_to_delete)
    db_manager.session.commit()


def test_run_process_orchestration(
    db_manager,
    grin_client,
    setup_barcodes_conversion
):
    test_barcodes = setup_barcodes_conversion 

    mock_convert_responses_for_pending = [
        "Barcode\tStatus",
        f"{test_barcodes[0]}\tSuccess",
        f"{test_barcodes[1]}\tAlready being converted",
        f"{test_barcodes[2]}\tAlready available for download",
        f"{test_barcodes[3]}\tNot allowed to be downloaded",
        f"{test_barcodes[4]}\tSome Other Unknown Status"
    ]

    mock_converted_filenames_response = [
        f"{test_barcodes[1]}.tar.gz.gpg",
        "some_other_file.tar.gz.gpg"
    ]

    with patch.dict(os.environ, {"GRIN_INGEST_SQS_QUEUE": "mock_queue", "SSM_PARAM_NAME": "/test/service/account/key"}):
        with patch.object(grin_client, 'convert') as mock_convert_method:
            mock_convert_method.return_value = mock_convert_responses_for_pending
            
            with patch.object(grin_client, 'converted_filenames') as mock_converted_filenames_method:
                mock_converted_filenames_method.return_value = mock_converted_filenames_response

                with patch('boto3.client') as mock_boto_client:
                    def boto_client_side_effect(service_name, *args, **kwargs):
                        if service_name == 'sqs':
                            mock_sqs_client_instance = MagicMock()
                            mock_sqs_client_instance.get_queue_url.return_value = {'QueueUrl': 'mock-sqs-url'}
                            mock_sqs_client_instance.send_message_to_queue.return_value = {}
                            return mock_sqs_client_instance
                        elif service_name == 'ssm':
                            mock_ssm_client_instance = MagicMock()
                            
                            dummy_private_key_pem = (
                               """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCgEIxp53FVl7T
jDZEIdx4QCaEy6uztyYKSnLB+n9/s2h/94tiplsFPnLH+pIhoZBG1JhBDMnEzMYY
G66X4xSkGta2JBrkHvVkQTMnvnrKxjDtoquOogSF+s4PHak67+ZtFPCylqYFdU+w
o1lgL3BuCM/ncYhtCc2idxNurK9Wdy26E9w029T6VnqhHhGF+A/BZ64Si1oGwOkD
P8XEQRhPdqvWxrrrFJ+X3EZS+/8iiVhmD4Pqsny4aqgR+fzFjfd7g0eo0s910i47
NqYZ8e2J1H+ZI7EyvbF0U/+HY2q4mC4zPMLDichODHtfpOhUmyBeXO5PJwUCq3/5
jls5I66nAgMBAAECggEAEggJRZ3axDL9TYgKdocxGjt67AVkYJ/SY1KC5bV5tnav
xxVFkxqwW4HDla3cIvQIcm3pGOFnnzALZAFvo93FycTsh4GSC033b+4/dige2Bzx
kkY/Y1S6g0qSFIKqcAVSNmX3k9w1ffy3K5KpNCqRthG+ZyAhBq10UTh60QrUonj2
iLmebxpafETC8MveAMK0PTWdi8OI2cpsj+kEY5ljxuPCEoBUfWUmpBMU4dSgKWmP
I6BPUlWKpGFIQ4urGSUC9GIJLwcIKvQOZEWIoYzUw1DDkYqusT4yqrMOFTH0aA2j
9Y53Sqm7rS5I2go+ShOSEVXdLh8CrxAK/x+ndNWDIQKBgQDuvt3K48YiLU+5nIDS
QtnlkqHY8cqgYRQpZXt3vxCLB+WE9Unbc99LgiQITOGXA3thMFIepMTkRH9UXxmL
WMlsLnMCyLSfD5oHjYZhtItU3dkW2qBK2uPOZEg/QBCt/W1K/+9PO6eDLg+KqjDL
xKYwbbT0uwdhucy0G/W2rYD31wKBgQDQjs4ANR2qUcxfrRCxNjHT8w8k6oyl7Z3C
9X8FkFDPyQ3m7CxWtMFJTYElKu+jxOzElWVnvv0S/+VYPhg3lLxIDO8c31KDS0kT
LyGdfqJmuKSAwzGa9ZFyNvLQ8Il1rTgBTgbIYsaSpLrbUYhg+gDQN1V+koAX/w+O
9YQHOEPlsQKBgHVjo+p+1I05elnpee3osPsQfkQNn3P8R82S+IKIj7nMyC337bjZ
4JFgDBeIteNq8t92wuoOWkFi7Livif/aSC/JJwPXa/hJ05KjI9Am1duEuZljJi2o
MxrodB2lgo4KbhLShPiQfG0j2MB1rkiDCLQHPVKYI6kJkn18wfRwm1lBAoGAZo8T
Nn7oS61V92a/4qVn83Z/aAP/jkk/X3QiNrY1RzjzoS9aznis5EM80u4+Uiaw2Csv
ZslA4mr8eVxvxEVcIYJaw7P+e5o2ITz4Jt7zNdhu7PMQHcfM8oGa/qyKrFe2Rs37
/+azB8ICMX/ytN28MKhFXqzkWOiQ2hhaCMLegMECgYEAle0tKPQoty3ynbvChqLM
Q7jM1fhr9F8YHqN9Md1J80jeaJytNZz7r8k8LT77ZsV1GxKiuHPi8qoRLZdA0L+e
lQ413Mru1N5u/nVpXDyvgoARAU4FA7vF8hFZHfGlLsjIL8GeYrRtWKwGyrotPCQp
tnxtH4Z7SsJgM0cuobL/UXY=
-----END PRIVATE KEY-----""") 

                            dummy_google_creds_json_str = json.dumps({
                                "type": "service_account",
                                "project_id": "mock-project-id",
                                "private_key_id": "mock_private_key_id",
                                "private_key": "-----BEGIN PRIVATE KEY-----\nMIICJQIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAoGB\n-----END PRIVATE KEY-----\n",
                                "client_email": "mock-service-account@mock-project-id.iam.gserviceaccount.com",
                                "client_id": "12345678901234567890",
                                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                                "token_uri": "https://oauth2.googleapis.com/token",
                                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mock-service-account%40mock-project-id.iam.gserviceaccount.com"
                            })
                            
                            mock_ssm_client_instance.get_parameter.return_value = {
                                'Parameter': {
                                    'Name': os.environ["SSM_PARAM_NAME"],
                                    'Type': 'String',
                                    'Value': dummy_google_creds_json_str,
                                    'Version': 1,
                                    'LastModifiedDate': datetime(2025, 1, 1),
                                    'ARN': 'arn:aws:ssm:us-east-1:123456789012:parameter/test/service/account/key',
                                    'DataType': 'text'
                                }
                            }
                            return mock_ssm_client_instance
                        else:
                            return MagicMock() 

                    mock_boto_client.side_effect = boto_client_side_effect
                    
                    with patch('time.sleep', MagicMock()):
                        mock_params = MagicMock(process_type="looping_test")
                        with patch('processes.grin.conversion.utils.parse_process_args') as mock_parse_args:
                            mock_parse_args.return_value = mock_params

                            with patch.object(GRINConversion, '_get_unconverted_barcode_count') as mock_get_count:
                                mock_get_count.side_effect = [1, 0]


                                conversion_process = GRINConversion(grin_client) 
                                
                                conversion_process.runProcess() 

                                status_0 = db_manager.session.query(GRINStatus).filter_by(barcode=test_barcodes[0]).first()
                                assert status_0 is not None
                                assert status_0.state == GRINState.CONVERTING.value, \
                                    f"Barcode {test_barcodes[0]} (PENDING->CONVERTING) failed. Got: {status_0.state}"

                                status_1 = db_manager.session.query(GRINStatus).filter_by(barcode=test_barcodes[1]).first()
                                assert status_1 is not None
                                assert status_1.state == GRINState.CONVERTED.value, \
                                    f"Barcode {test_barcodes[1]} (CONVERTING->CONVERTED) failed. Got: {status_1.state}"

                                status_2 = db_manager.session.query(GRINStatus).filter_by(barcode=test_barcodes[2]).first()
                                assert status_2 is not None
                                assert status_2.state == GRINState.CONVERTED.value, \
                                    f"Barcode {test_barcodes[2]} (CONVERTED) changed unexpectedly. Got: {status_2.state}"

                                status_3 = db_manager.session.query(GRINStatus).filter_by(barcode=test_barcodes[3]).first()
                                assert status_3 is not None
                                assert status_3.state == GRINState.DOWNLOADED.value, \
                                    f"Barcode {test_barcodes[3]} (DOWNLOADED) changed unexpectedly. Got: {status_3.state}"

                                status_4 = db_manager.session.query(GRINStatus).filter_by(barcode=test_barcodes[4]).first()
                                assert status_4 is not None
                                assert status_4.state == GRINState.UNAVAILABLE.value, \
                                    f"Barcode {test_barcodes[4]} (UNAVAILABLE) changed unexpectedly. Got: {status_4.state}"

                                mock_sqs_client_instance_from_call = mock_boto_client.side_effect('sqs') 
                                expected_sqs_message = {"barcodes": [test_barcodes[1]]} 
                                mock_sqs_client_instance_from_call.send_message_to_queue.assert_called_once_with(expected_sqs_message)