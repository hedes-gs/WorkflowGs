import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'

export interface ExifImagesService {
    getExifDataOfImage(exifUrl: string | undefined): Promise<string>;

}

export default class ExifImagesServiceImpl implements ExifImagesService {
    axiosInstance: AxiosInstance;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
    }

    async getExifDataOfImage(
        exifUrl: string): Promise<string> {

        return this.axiosInstance.get(exifUrl)
            .then(resp => resp.data);
    }

}
