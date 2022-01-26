import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'
import { ExchangedImageDTO, toJsonExchangedImageDTO } from '../model/DataModel';

export interface PersonsService {
    addPerson(Person: string, img: ExchangedImageDTO): Promise<string>;
    deletePerson(Person: string, img: ExchangedImageDTO): Promise<string>;
    getPersonsLike(Person: string): Promise<string>;
    getAll(): Promise<string>;
}

export default class PersonsServiceImpl implements PersonsService {

    axiosInstance: AxiosInstance;
    baseUrlForPersons: string;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlForPersons = '/api/gs/persons/';
    }

    async getPersonsLike(Person: string): Promise<string> {
        const urlToGetPersonsLike = this.baseUrlForPersons + 'getPersonsLike/' + Person;
        return this.axiosInstance.get(urlToGetPersonsLike)
            .then(resp => resp.data);
    }

    async addPerson(Person: string, img: ExchangedImageDTO): Promise<string> {
        const urlToAddPerson = this.baseUrlForPersons + 'addToImage/' + Person;

        return this.axiosInstance.post(urlToAddPerson, toJsonExchangedImageDTO(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async deletePerson(Person: string, img: ExchangedImageDTO): Promise<string> {
        const urlToAddPerson = this.baseUrlForPersons + 'deleteInImage/' + Person;

        return this.axiosInstance.post(urlToAddPerson, toJsonExchangedImageDTO(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async getAll(): Promise<string> {
        const urlToGetRating = this.baseUrlForPersons + 'all';
        return this.axiosInstance.get(urlToGetRating)
            .then(resp => resp.data);
    }


}
