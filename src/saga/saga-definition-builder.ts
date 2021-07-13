import {SagaProcessor} from './saga-processor';

export enum STEP_PHASE {
    STEP_FORWARD = 'STEP_FORWARD',
    STEP_BACKWARD = 'STEP_BACKWARD'
}

export type SagaMessage<P = any> = {
    payload: P,
    saga: {
        index: number,
        phase: STEP_PHASE
    }
}

export type Command<P = any, RES = void> = (payload?: P) => Promise<RES>;

export type SagaDefinition = {
    channelName: string,
    phases: { [key in STEP_PHASE]?: { command: Command } }
}


export class SagaDefinitionBuilder {
    index: number | null = null;
    sagaDefinitions: SagaDefinition[] = [];

    step(channelName: string): SagaDefinitionBuilder {
        this.index = this.index === null ? 0 : this.index + 1;
        this.sagaDefinitions = [...this.sagaDefinitions, {channelName, phases: {}}];
        return this;
    }


    onReply(command: Command): SagaDefinitionBuilder {
        this.checkIndex();
        this.sagaDefinitions[this.index!].phases[STEP_PHASE.STEP_FORWARD] = {command};
        return this;
    }

    withCompensation(command: Command): SagaDefinitionBuilder {
        this.checkIndex();
        this.sagaDefinitions[this.index!].phases[STEP_PHASE.STEP_BACKWARD] = {command};
        return this;
    }

    private checkIndex() {
        if (this.index === null) {
            throw new Error('before build saga definition, you need to invoke step function before');
        }
    }

    async build(): Promise<SagaProcessor> {
        const sagaProcessor = new SagaProcessor(this.sagaDefinitions);
        await sagaProcessor.init();
        return sagaProcessor;
    };
}