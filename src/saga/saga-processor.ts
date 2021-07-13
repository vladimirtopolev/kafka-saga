import {Kafka, ITopicConfig} from 'kafkajs';
import {SagaDefinition, SagaMessage, STEP_PHASE} from './saga-definition-builder';


const kafka = new Kafka({brokers: ['localhost:9092']});
const admin = kafka.admin();

export class SagaProcessor {
    producer = kafka.producer();
    consumer = kafka.consumer({groupId: 'saga'});

    constructor(private sagaDefinitions: SagaDefinition[]) {}

    async init() {
        await admin.connect();
        await this.producer.connect();
        await this.consumer.connect();

        const stepTopics = this.sagaDefinitions.map((definition) => definition.channelName);

        const kafkaTopics = stepTopics.map((topic): ITopicConfig => ({topic}));
        await admin.createTopics({topics: kafkaTopics});
        console.log('Saga topics created successfully');

        for (let topic of stepTopics) {
            await this.consumer.subscribe({topic});
        }

        await this.consumer.run({
            eachMessage: async ({topic, message, partition}) => {
                const sagaMessage = JSON.parse(message.value!.toString()) as SagaMessage;

                const {saga, payload} = sagaMessage;
                const {index, phase} = saga;

                console.log('===> message received', saga, 'payload', payload);

                switch (phase) {
                    case STEP_PHASE.STEP_FORWARD: {
                        const stepForward = this.sagaDefinitions[index].phases[STEP_PHASE.STEP_FORWARD]!.command;
                        try {
                            await stepForward();
                            await this.makeStepForward(index + 1, payload);
                        } catch (e) {
                            await this.makeStepBackward(index - 1, payload);
                        }
                        return;
                    }
                    case STEP_PHASE.STEP_BACKWARD: {
                        const stepBackward = this.sagaDefinitions[index].phases[STEP_PHASE.STEP_BACKWARD]!.command;
                        await stepBackward();
                        await this.makeStepBackward(index - 1, payload);
                        return;
                    }
                    default: {
                        console.log('UNAVAILBLE SAGA PHASE');
                    }
                }
            }
        });
    }

    async makeStepForward(index: number, payload: any) {
        if (index >= this.sagaDefinitions.length) {
            console.log('====> Saga finished and transaction successful');
            return;
        }
        const message = {payload, saga: {index, phase: STEP_PHASE.STEP_FORWARD}};
        await this.producer.send({
            topic: this.sagaDefinitions[index].channelName,
            messages: [
                {value: JSON.stringify(message)}
            ]
        });
    }

    async makeStepBackward(index: number, payload: any) {
        if (index < 0) {
            console.log('===> Saga finished and transaction rolled back');
            return;
        }
        await this.producer.send({
            topic: this.sagaDefinitions[index].channelName,
            messages: [
                {value: JSON.stringify({payload, saga: {index, phase: STEP_PHASE.STEP_BACKWARD}})}
            ]
        });
    }


    async start(payload: any) {
        await this.makeStepForward(0, payload);
        console.log('Saga started');
    }
}