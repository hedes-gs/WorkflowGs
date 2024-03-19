import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'
import { ImportEvent, toJson } from '../model/WfEvents'

export interface WfEventsServices {
    startScan(event: ImportEvent): Promise<string>;
}

export default class WfEventsServicesImpl implements WfEventsServices {

    axiosInstance: AxiosInstance;
    baseUrlToStartScan: string;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlToStartScan = '/api/gs/import/start';
    }

    async startScan(event: ImportEvent): Promise<string> {
        return this.axiosInstance.post(this.baseUrlToStartScan, toJson(event), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

}
